# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Chris Ferebee
"""
date_recovery.py — v0.1

Date recovery from file metadata for the beachcomb tool.
"""
import datetime as dt
from pathlib import Path
from typing import Optional, Tuple

from utils import run, which

def exiftool_date(path: Path):
    if not which("exiftool"):
        return (None, None)
    tags = [
        "-SubSecDateTimeOriginal","-DateTimeOriginal","-CreateDate","-XMP:CreateDate",
        "-QuickTime:CreateDate","-api","QuickTimeUTC=1","-s","-s","-s", str(path)
    ]
    rc, out, _ = run(["exiftool"] + tags, timeout=20)
    if rc != 0:
        return (None, None)
    for val in [l.strip() for l in out.splitlines() if l.strip()]:
        try:
            if len(val)>=10 and val[4]==":" and val[7]==":":
                val = f"{val[0:4]}-{val[5:7]}-{val[8:10]}{val[10:]}"
            d = dt.datetime.fromisoformat(val.replace(" ","T"))
            if d.tzinfo is None: d = d.replace(tzinfo=dt.timezone.utc)
            return ("exif", d.astimezone())
        except Exception:
            continue
    return (None,None)

def video_date(path: Path):
    if not which("exiftool"):
        return (None, None)
    tags = ["-api","QuickTimeUTC=1","-MediaCreateDate","-TrackCreateDate","-CreateDate","-DateTimeOriginal","-s","-s","-s", str(path)]
    rc, out, _ = run(["exiftool"]+tags, timeout=20)
    if rc != 0: 
        return (None,None)
    for line in out.splitlines():
        v = line.strip()
        if not v: continue
        try:
            v = v.replace(" ","T")
            d = dt.datetime.fromisoformat(v)
            if d.tzinfo is None: d = d.replace(tzinfo=dt.timezone.utc)
            return ("video_meta", d.astimezone())
        except Exception:
            continue
    return (None,None)

def ffprobe_date_and_duration(path: Path):
    if not which("ffprobe"):
        return (None, None, None)
    rc1, ct, _ = run(["ffprobe","-v","error","-show_entries","format_tags=creation_time","-of","default=nk=1:nw=1", str(path)], timeout=15)
    rc2, dur, _ = run(["ffprobe","-v","error","-show_entries","format=duration","-of","default=nk=1:nw=1", str(path)], timeout=15)
    dtn = None
    if rc1==0 and ct.strip():
        try:
            dtn = dt.datetime.fromisoformat(ct.strip().replace(" ","T"))
            if dtn.tzinfo is None: dtn = dtn.replace(tzinfo=dt.timezone.utc)
            dtn = dtn.astimezone()
        except Exception:
            dtn = None
    duration = None
    try:
        duration = float(dur.strip()) if rc2==0 and dur.strip() else None
    except Exception:
        duration = None
    return ("ffprobe_creation_time" if dtn else None), dtn, duration

def _parse_pdfinfo_date_line(line: str) -> Optional[dt.datetime]:
    """Helper to parse a single date line from pdfinfo output."""
    try:
        val = line.split(":", 1)[1].strip()
        d = dt.datetime.fromisoformat(val)
        if d.tzinfo is None:
            d = d.replace(tzinfo=dt.timezone.utc)
        return d.astimezone()
    except (ValueError, IndexError):
        return None

def pdfinfo_dates(path: Path) -> Tuple[Optional[str], Optional[dt.datetime]]:
    """Extracts creation or modification date from PDF metadata using pdfinfo."""
    if not which("pdfinfo"):
        return None, None
    rc, out, _ = run(["pdfinfo", "-isodates", str(path)], timeout=20)
    if rc != 0:
        return None, None
    creation_date: Optional[dt.datetime] = None
    mod_date: Optional[dt.datetime] = None
    for line in out.splitlines():
        if line.startswith("CreationDate:"):
            creation_date = _parse_pdfinfo_date_line(line)
        elif line.startswith("ModDate:"):
            mod_date = _parse_pdfinfo_date_line(line)
    if creation_date:
        return "pdf_creationdate", creation_date
    if mod_date:
        return "pdf_moddate", mod_date
    return None, None
