# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Chris Ferebee
"""
utils.py — v0.1

Small, general-purpose utilities for the beachcomb tool.
"""
import datetime as dt
import subprocess
import shutil
import sys
from pathlib import Path
from typing import List, Optional, Tuple

def log(msg: str):
    ts = dt.datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

def run(cmd: List[str], timeout: int = 20) -> Tuple[int, str, str]:
    try:
        p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout, check=False)
        stdout_str = p.stdout.decode('utf-8', errors='replace')
        stderr_str = p.stderr.decode('utf-8', errors='replace')
        return p.returncode, stdout_str, stderr_str
    except subprocess.TimeoutExpired:
        return 124, "", "timeout"
    except FileNotFoundError as e:
        log(f"ERROR: Command not found: '{cmd[0]}'. Please ensure it's installed and in your PATH.")
        return 127, "", str(e)

def which(prog: str) -> Optional[str]:
    return shutil.which(prog)

def zip_list_contents(p: Path) -> List[str]:
    if which("zipinfo"):
        rc, out, _ = run(["zipinfo","-1", str(p)], timeout=10)
        if rc == 0:
            return out.splitlines()
    return []

def ensure_unique_path(p: Path) -> Path:
    if not p.exists():
        return p
    parent, stem, suffix = p.parent, p.stem, p.suffix
    n = 1
    while True:
        cand = parent / f"{stem} ({n}){suffix}"
        if not cand.exists():
            return cand
        n += 1

def is_apfs(path: Path) -> bool:
    try:
        rc, out, _ = run(["/usr/sbin/diskutil", "info", str(path.resolve())], timeout=10)
        if rc == 0 and "Type (Bundle)" in out and "apfs" in out.lower():
            return True
    except Exception:
        pass
    return False

def parse_iso_datetime(s: str) -> dt.datetime:
    d = dt.datetime.fromisoformat(s)
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc).astimezone()
    return d

def file_mtime(path: Path) -> dt.datetime:
    ts = path.stat().st_mtime
    return dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc).astimezone()

def read_head_tail_signature(path: Path, block_size: int = 256*1024) -> Tuple[int, bytes, bytes]:
    size = path.stat().st_size
    with open(path, "rb") as f:
        head = f.read(block_size)
        if size > block_size:
            f.seek(max(0, size - block_size))
            tail = f.read(block_size)
        else:
            tail = b""
    return size, head, tail

TOOL_PACKAGES = {
    "exiftool": "exiftool",
    "ffprobe": "ffmpeg",
    "ffmpeg": "ffmpeg",
    "pdfinfo": "poppler",
    "pdffonts": "poppler",
    "pdftotext": "poppler",
    "qpdf": "qpdf",
    "mutool": "mupdf-tools",
    "gs": "ghostscript",
    "ocrmypdf": "ocrmypdf",
    "b3sum": "b3sum",
    "zipinfo": "unzip",
    "ssconvert": "gnumeric",
}

def check_dependencies(tools: List[str]):
    """Checks for required command-line tools and exits if they are not found."""
    missing = [tool for tool in tools if not which(tool)]
    if not missing:
        return

    print("Error: Missing required command-line tools.", file=sys.stderr)
    print("This script relies on external programs to function.", file=sys.stderr)

    packages = sorted(list(set(TOOL_PACKAGES[tool] for tool in missing if tool in TOOL_PACKAGES)))

    print("\nTo install the missing tools using Homebrew, run this command:", file=sys.stderr)
    print(f"    brew install {' '.join(packages)}", file=sys.stderr)
    sys.exit(1)
