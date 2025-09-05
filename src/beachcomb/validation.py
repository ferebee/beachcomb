# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Chris Ferebee
"""
validation.py

File integrity validation and repair for the beachcomb tool.
"""
import os
import traceback
from pathlib import Path
from typing import List, Tuple, Optional

from .utils import run, which, log, zip_list_contents

# ------------------------- PDF helpers -------------------------

def parse_pdfinfo_meta(path: Path) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if not which("pdfinfo"):
        return None, None, None
    rc, out, _ = run(["pdfinfo", str(path)], timeout=20)
    if rc != 0:
        return None, None, None
    ver = enc = lin = None
    for line in out.splitlines():
        if line.startswith("PDF version:"):
            ver = line.split(":",1)[1].strip()
        elif line.startswith("Encrypted:"):
            enc = line.split(":",1)[1].strip()
        elif line.startswith("Linearized:"):
            lin = line.split(":",1)[1].strip()
    return ver, enc, lin

def pdf_has_eof_tail(path: Path) -> bool:
    try:
        with open(path, "rb") as f:
            f.seek(-65536, os.SEEK_END)
            tail = f.read(65536)
        return b"%%EOF" in tail
    except Exception:
        return False

def pdf_deep_check(path: Path) -> Tuple[bool, str]:
    if which("qpdf"):
        rc, _, _ = run(["qpdf","--check", str(path)], timeout=60)
        if rc != 0:
            return False, "qpdf-check-fail"
    if which("mutool"):
        rc, out, err = run(["mutool","info", str(path)], timeout=30)
        if rc != 0 and "cannot open document" in (err.lower()+out.lower()):
            return False, "mutool-info-fail"
    if which("gs"):
        rc, _, _ = run(["gs","-o","/dev/null","-sDEVICE=nullpage","-dBATCH","-dNOPAUSE","-q", str(path)], timeout=60)
        if rc != 0:
            return False, "gs-render-fail"
    return True, ""

def pdf_try_repair(src: Path, dest: Path) -> bool:
    if not which("qpdf"):
        return False
    dest.parent.mkdir(parents=True, exist_ok=True)
    rc, _, _ = run(["qpdf","--repair","--object-streams=preserve", str(src), str(dest)], timeout=120)
    return rc == 0 and dest.exists()

def ocr_pdf(src: Path, dest: Path, lang: str) -> bool:
    if not which("ocrmypdf"):
        return False
    tmp = dest.with_suffix(".ocr.tmp.pdf")
    dest.parent.mkdir(parents=True, exist_ok=True)
    rc, _, _ = run(["ocrmypdf", "--skip-text", "--optimize", "1", "--fast-web-view", "1",
                        "--language", lang, str(src), str(tmp)], timeout=3600)
    if rc == 0 and tmp.exists():
        try:
            tmp.replace(dest)
            return True
        except Exception:
            return False
    return False

# ------------------------- Office integrity helpers -------------------------

def _zip_ok(p: Path) -> bool:
    if which("unzip"):
        rc, _, _ = run(["unzip","-t","-qq", str(p)], timeout=15)
        return rc == 0
    if which("zipinfo"):
        rc, _, _ = run(["zipinfo","-t", str(p)], timeout=15)
        return rc == 0
    return True

def _xlsx_core_present(p: Path) -> bool:
    items = set(zip_list_contents(p))
    if not items:  # tool missing → don’t fail here
        return True
    req = {"[Content_Types].xml", "xl/workbook.xml"}
    return req.issubset(items) and any(x.startswith("xl/worksheets/") for x in items)

def _ole_has_workbook(p: Path) -> bool:
    try:
        with open(p, "rb") as f:
            sig = f.read(8)
        if sig != b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1":
            return False
    except Exception:
        return False
    rc, out, _ = run(["file","-b", str(p)], timeout=5) if which("file") else (0,"","")
    return (rc==0) and ("Composite Document File" in out or "CDFV2" in out)

def _numbers_package_ok(p: Path) -> Tuple[bool,str]:
    if p.is_dir():
        idx = p / "Index.zip"
        if idx.exists():
            if not _zip_ok(idx): 
                return False, "numbers-indexzip-fail"
            return True, ""
        if (p/"Index").exists(): 
            return True, ""
        if (p/"QuickLook/Preview.pdf").exists():
            return False, "iwork-preview-only"
        return False, "numbers-missing-index"
    else:
        if not _zip_ok(p): 
            return False, "numbers-zip-fail"
        return True, ""

def classify_zip_contents(p: Path) -> str:
    """Peeks into a ZIP file and classifies it based on its contents."""
    contents = zip_list_contents(p)
    if not contents:
        return "ZIP"

    extensions = [Path(item).suffix.lower() for item in contents if Path(item).suffix]
    if not extensions:
        return "ZIP"

    doc_exts = {".pdf", ".docx", ".doc", ".xlsx", ".xls", ".pptx", ".ppt", ".txt", ".rtf", ".pages", ".key", ".numbers"}
    img_exts = {".jpg", ".jpeg", ".png", ".gif", ".tif", ".tiff", ".bmp", ".heic", ".webp"}
    web_exts = {".html", ".htm", ".js", ".css"}
    app_exts = {".dylib", ".dll", ".so", ".exe", ".app"}

    # Check for strong signals first
    if any(ext in app_exts for ext in extensions):
        return "ZIP-AppFiles"
    if any(ext in web_exts for ext in extensions):
        return "ZIP-WebApp"

    doc_count = sum(1 for ext in extensions if ext in doc_exts)
    img_count = sum(1 for ext in extensions if ext in img_exts)

    total_classified = doc_count + img_count
    if total_classified == 0:
        return "ZIP"

    if img_count > 0 and (img_count / total_classified) >= 0.75:
        return "ZIP-Images"

    return "ZIP-Documents" if doc_count > 0 else "ZIP"

# ------------------------- Video integrity & repair -------------------------

def _mp4_atom_presence(path: Path) -> Tuple[bool,str]:
    size = path.stat().st_size
    try:
        with open(path, "rb") as f:
            head = f.read(min(size, 1024*1024))
            tail = b""
            if size > 1024*1024:
                f.seek(max(0, size-1024*1024))
                tail = f.read(1024*1024)
        blob = head + tail
        have_ftyp = b"ftyp" in blob
        have_moov = b"moov" in blob
        have_mdat = b"mdat" in blob
        # If we see an MP4 signature but literally nothing else, it's likely a truncated header-only file
        if have_ftyp and not have_moov and not have_mdat:
            return False, "mp4-ftyp-only"
        # Otherwise, don't require 'mdat' to appear in head/tail; many valid files won't show it in this window, so we removed the following:
        # if have_moov and not have_mdat:
        #     return False, "mp4-no-mdat"
        return True, ""
    except Exception:
        return False, "mp4-atom-read-fail"

def try_ffmpeg_rewrap(src: Path, dest: Path) -> bool:
    if not which("ffmpeg"):
        return False
    dest.parent.mkdir(parents=True, exist_ok=True)
    rc, _, _ = run(["ffmpeg","-v","error","-y","-i", str(src),
                    "-map","0","-c","copy","-movflags","+faststart", str(dest)], timeout=900)
    return rc == 0 and dest.exists()

def ffmpeg_smoke(path: Path) -> bool:
    if not which("ffmpeg"):
        return True
    # The "-t 1" option is more compatible with older ffmpeg versions than "-read_intervals".
    rc, _, err = run(["ffmpeg","-v","error","-i", str(path), "-t", "1", "-f","null","-","-y"], timeout=120)
    return rc == 0

# ------------------------- integrity checks dispatcher -------------------------

def quick_integrity(path: Path, family: str, subtype: str, mode: str,
                    office_deep: bool, video_decode_smoke: bool) -> Tuple[str, dict]:
    """Return (integrity_tag, extra_info_dict)"""
    try:
        if family == "PDFs":
            rc, _, _ = run(["pdfinfo", str(path)], timeout=10) if which("pdfinfo") else (0,"","")
            # A failure to read with pdfinfo is fatal and means the file is truly damaged.
            if rc != 0:
                return "pdfinfo-fail", {}
            # Other issues are treated as non-fatal warnings.
            pdf_error_tag = ""
            if not pdf_has_eof_tail(path):
                pdf_error_tag = "pdf-tail-fail"
            elif mode == "heavy":
                ok, tag = pdf_deep_check(path)
                if not ok:
                    pdf_error_tag = tag
            return "ok", {"pdf_error": pdf_error_tag}
        elif family == "Video":
            if which("ffprobe"):
                rc, _, _ = run(["ffprobe","-v","error","-show_streams","-show_format","-of","json", str(path)], timeout=12)
                if rc != 0:
                    return "ffprobe-fail", {}
                rc, dur, _ = run(["ffprobe","-v","error","-show_entries","format=duration","-of","default=nk=1:nw=1", str(path)], timeout=8)
                try:
                    duration = float(dur.strip()) if dur.strip() else 0.0
                except Exception:
                    duration = 0.0
                if duration <= 0.0:
                    return "ffprobe-no-duration", {"video_duration":"0"}
                if video_decode_smoke:
                    if not ffmpeg_smoke(path):
                        return "ffmpeg-decode-error", {"video_duration":str(duration)}
                return "ok", {"video_duration":str(duration)}
            else:
                return "tool-missing", {}

        elif family == "Archives" or family == "GIS":
            if subtype == "ZIP" and which("unzip"):
                rc, _, _ = run(["unzip","-t","-qq", str(path)], timeout=10)
                integrity_tag = "ok" if rc == 0 else "zip-test-fail"
                new_subtype = classify_zip_contents(path) if integrity_tag == "ok" else "ZIP"
                return (integrity_tag, {"archive_subtype": new_subtype})
            if subtype == "KMZ":
                # A KMZ is a ZIP file, so we can test its integrity the same way.
                return ("ok" if _zip_ok(path) else "zip-test-fail", {})
            return "ok", {}

        elif family == "Office" and office_deep:
            ext = path.suffix.lower()
            if ext in (".xlsx",".xlsm",".xltx"):
                if not _zip_ok(path): return "xlsx-zip-fail", {"office_error":"zip-fail"}
                if not _xlsx_core_present(path): return "xlsx-missing-core", {"office_error":"missing-core"}
                return "ok", {}
            if ext == ".xls":
                return ("ok", {}) if _ole_has_workbook(path) else ("xls-ole-fail", {"office_error":"ole-missing"})
            if ext == ".numbers":
                ok, tag = _numbers_package_ok(path)
                return ("ok", {}) if ok else (tag, {"office_error":tag})
            return "ok", {}

        else:
            with open(path, "rb") as f:
                sig = f.read(12)
                if len(sig) < 4:
                    return "bad-header", {}
            return "ok", {}
    except Exception as e:
        log(f"Unhandled exception in quick_integrity for {path}: {e}\n{traceback.format_exc()}")
        return "error", {}
