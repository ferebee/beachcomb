# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Chris Ferebee
"""
exiftoold.py — v0.2
Robust, single-owner ExifTool daemon wrapper for beachcomb.

Design:
- One background thread owns the exiftool -stay_open process and its pipes.
- Requests serialized via a Queue; each includes a unique -execute{id}.
- We append an -echo3 "{ready} <id>" marker and read stdout until that line.
- On timeout/EOF/broken pipe, we kill/restart and return a nonzero rc so
  callers can treat it as "exiftool failed" and continue.
- Public API: exiftool(args: list[str], timeout=20.0) -> (rc, out, err_like)
"""

from __future__ import annotations
import os, sys, time, threading, subprocess, queue, shlex
from dataclasses import dataclass, field
from typing import List, Tuple, Optional

from .utils import which, log

_READY_PREFIX = "{ready} "
_DEFAULT_COMMON_ARGS = [
    "-q", "-q",          # quiet x2
    "-n",                # numeric values
    "-S",                # simple output (one tag per line)
    "-charset", "filename=UTF8",
    "-charset", "exiftool=UTF8",
]
DEBUG = os.getenv("EXIFTOOLD_DEBUG") not in (None, "", "0", "false", "False")
TRACE_IO = os.getenv("EXIFTOOLD_TRACE_IO") not in (None, "", "0", "false", "False")

def dlog(msg: str):
    if DEBUG:
        log(f"[exiftoold] {msg}")

def tlog(prefix: str, line: str):
    if TRACE_IO:
        # show control characters minimally normalized
        log(f"[exiftoold:{prefix}] {line!r}")

@dataclass
class _Request:
    args: List[str]
    timeout: float
    req_id: int
    done: threading.Event = field(default_factory=threading.Event)
    result: Tuple[int, str, str] | None = None  # (rc, stdout, err_like)

class ExifToolDaemon:
    def __init__(self, max_queue: int = 256):
        self._q: "queue.Queue[_Request]" = queue.Queue(maxsize=max_queue)
        self._proc: Optional[subprocess.Popen] = None
        self._lock = threading.Lock()
        self._next_id = 1
        self._stopping = False
        self._owner = threading.Thread(target=self._worker, name="exiftoold", daemon=True)
        self._owner.start()
        dlog("owner thread started")

    # ---------- process lifecycle ----------
    def _spawn(self):
        exe = which("exiftool")
        if not exe:
            raise FileNotFoundError("exiftool not found in PATH")

        # Merge stderr into stdout so marker drain works deterministically.
        # Use text mode for easy line delimiters.
        self._proc = subprocess.Popen(
            [exe, "-stay_open", "True", "-@", "-", "-common_args"] + _DEFAULT_COMMON_ARGS,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=1,            # line-buffered
            universal_newlines=True,
            encoding="utf-8",
        )
        dlog(f"spawned exiftool pid={self._proc.pid}")

    def _kill(self):
        if not self._proc:
            return
        try:
            if self._proc.stdin:
                try:
                    # Graceful close for -stay_open
                    self._proc.stdin.write("-stay_open\nFalse\n")
                    self._proc.stdin.flush()
                    tlog(">>", "-stay_open\\nFalse")
                except Exception:
                    pass
            self._proc.kill()
        except Exception:
            pass
        finally:
            dlog(f"killed exiftool pid={getattr(self._proc, 'pid', None)}")
            self._proc = None

    def _restart(self):
        dlog("restarting exiftool...")
        self._kill()
        self._spawn()

    # ---------- main worker ----------
    def _worker(self):
        while not self._stopping:
            try:
                req = self._q.get(timeout=0.1)
            except queue.Empty:
                continue

            try:
                if self._proc is None or self._proc.poll() is not None:
                    self._spawn()

                ready_token = f"{_READY_PREFIX}{req.req_id}"
                # Build per-request argument stream for -@ -
                lines: List[str] = []
                for a in req.args:
                    # Each CLI arg must be its own line
                    s = str(a)
                    if "\n" in s:
                        # Extremely rare, but keep the stream sane
                        s = s.replace("\r", " ").replace("\n", " ")
                    lines.append(s + "\n")
                # Append marker and execute with ID
                lines.append("-echo3\n");                lines.append(ready_token + "\n")
                lines.append(f"-execute{req.req_id}\n")

                # Write request
                if not self._proc.stdin:
                    raise BrokenPipeError("stdin is closed")
                for L in lines:
                    tlog(">>", L.rstrip("\n"))
                    self._proc.stdin.write(L)
                self._proc.stdin.flush()

                # Read until marker or timeout
                if not self._proc.stdout:
                    raise BrokenPipeError("stdout is closed")

                out_lines: List[str] = []
                start = time.time()
                while True:
                    remaining = req.timeout - (time.time() - start)
                    if remaining <= 0:
                        self._restart()
                        req.result = (124, "", "timeout")
                        dlog(f"req {req.req_id} timeout")
                        break

                    # readline is blocking; we emulate a timeout by loop checking
                    self._proc.stdout.flush()
                    line = self._proc.stdout.readline()
                    if line == "":  # EOF
                        self._restart()
                        req.result = (111, "", "eof")
                        dlog(f"req {req.req_id} EOF")
                        break

                    line = line.rstrip("\r\n")
                    tlog("<<", line)

                    if line == ready_token:
                        text = "\n".join(out_lines).strip()
                        # Heuristic rc: ExifTool doesn’t emit an rc; we check for "Error"
                        rc = 0
                        if any(l.lower().startswith("error") for l in out_lines):
                            rc = 1
                        req.result = (rc, text, "")
                        dlog(f"req {req.req_id} done rc={rc} bytes={len(text)}")
                        break
                    else:
                        out_lines.append(line)

            except (BrokenPipeError, OSError) as e:
                dlog(f"pipe error: {e!r}")
                self._restart()
                req.result = (32, "", "epipe")
            except FileNotFoundError as e:
                dlog("exiftool not found")
                req.result = (127, "", "exiftool-not-found")
            except Exception as e:
                dlog(f"exception: {e!r}")
                self._restart()
                req.result = (1, "", f"exception: {e}")
            finally:
                req.done.set()
                self._q.task_done()

    # ---------- public API ----------
    def call(self, args: List[str], timeout: float = 20.0) -> Tuple[int, str, str]:
        with self._lock:
            rid = self._next_id
            self._next_id += 1
        req = _Request(args=list(args), timeout=timeout, req_id=rid)
        dlog(f"enqueue req {rid}: {shlex.join(map(str, args))}")
        self._q.put(req, block=True)
        req.done.wait()
        return req.result if req.result else (1, "", "no-result")

    def stop(self):
        self._stopping = True
        self._kill()

_global: Optional[ExifToolDaemon] = None
_global_lock = threading.Lock()
_available_checked = False
_available = False

def available() -> bool:
    global _available_checked, _available
    if not _available_checked:
        _available_checked = True
        path = which("exiftool")
        _available = bool(path)
        dlog(f"available={_available} path={path}")
    return _available

def get() -> ExifToolDaemon:
    global _global
    with _global_lock:
        if _global is None:
            _global = ExifToolDaemon()
        return _global

def exiftool(args: List[str], timeout: float = 20.0) -> Tuple[int, str, str]:
    """
    Public convenience API: run ExifTool via the shared daemon.
    Pass ONLY exiftool options and paths (not the program name).
    Returns (rc, stdout, stderr-like-string).
    """
    if not available():
        return (127, "", "exiftool-not-found")
    sargs = [str(a) for a in args]
    return get().call(sargs, timeout=timeout)

# Optional quick smoke test (python -m beachcomb.exiftoold /path/to/file)
def selftest(path: Optional[str] = None) -> Tuple[int, str, str]:
    # version probe
    rc_v, out_v, err_v = exiftool(["-ver"], timeout=5)
    if rc_v != 0:
        return rc_v, out_v, err_v or "version probe failed"

    if path:
        rc, out, err = exiftool(["-s", "-G1", "-n", "-FileName", "-FileSize", "-DateTimeOriginal", path], timeout=10)
        return rc, out, err
    return rc_v, out_v, err_v

if __name__ == "__main__":
    p = sys.argv[1] if len(sys.argv) > 1 else None
    rc, out, err = selftest(p)
    print(out)
    if err:
        print(err, file=sys.stderr)
    sys.exit(rc)

