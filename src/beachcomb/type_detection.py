# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Chris Ferebee
"""
type_detection.py — v0.1

File type detection logic for the beachcomb tool.
"""

from pathlib import Path
from typing import Optional, Tuple

from utils import run, which

EXT_FAMILY = {
    # Images
    "jpg": ("Images","JPG"), "jpeg": ("Images","JPG"), "png": ("Images","PNG"),
    "heic": ("Images","HEIC"), "heif": ("Images","HEIC"), "jp2": ("Images","JP2"),
    "tif": ("Images","TIFF"), "tiff": ("Images","TIFF"), "gif": ("Images","GIF"),
    "webp": ("Images","WEBP"),
    # Video
    "mov": ("Video","MOV"), "mp4": ("Video","MP4"), "m4v": ("Video","M4V"), "avi": ("Video","AVI"),
    # Audio
    "m4a": ("Audio","M4A"), "mp3": ("Audio","MP3"), "wav": ("Audio","WAV"), "aif": ("Audio","AIFF"), "aiff": ("Audio","AIFF"),
    # PDFs
    "pdf": ("PDFs","PDF"),
    # Office / Documents
    "doc": ("Office","Word"), "docx": ("Office","Word"), "odt": ("Office","Writer"),
    "xls": ("Office","Excel"), "xlsx": ("Office","Excel"), "numbers": ("Office","Numbers"),
    "xlsm": ("Office","Excel"), "xltx": ("Office","Excel"),
    "ppt": ("Office","PowerPoint"), "pptx": ("Office","PowerPoint"), "key": ("Office","Keynote"),
    "rtf": ("Text","RTF"), "txt": ("Text","TXT"), "csv": ("Text","CSV"),
    # Adobe creative
    "psd": ("Adobe","Photoshop"), "ai": ("Adobe","Illustrator"), "eps": ("Adobe","EPS"),
    "indd": ("Adobe","InDesign"), "idml": ("Adobe","IDML"), "svg": ("Adobe","SVG"),
    # Archives
    "zip": ("Archives","ZIP"), "7z": ("Archives","7z"), "rar": ("Archives","RAR"),
    "tar": ("Archives","TAR"), "gz": ("Archives","GZIP"), "tgz": ("Archives","TARGZ"),
    # Other
    "ics": ("Other","ICS"), "sqlite": ("Other","SQLite"), "eml": ("Other","EML"), "mbox": ("Other","MBOX"),
    "kmz": ("GIS","KMZ"),
}

MIME_EXT = {
    "image/jpeg": "jpg", "image/png": "png", "image/gif": "gif", "image/webp": "webp",
    "image/tiff": "tiff", "image/heif": "heic", "image/heic": "heic", "image/jp2": "jp2",
    "application/pdf": "pdf", "text/rtf": "rtf", "text/plain": "txt", "text/csv": "csv",
    "application/postscript": "eps", "application/vnd.adobe.photoshop": "psd",
    "application/illustrator": "ai",
    "video/quicktime": "mov", "video/mp4": "mp4",
    "audio/mpeg": "mp3", "audio/wav": "wav", "audio/mp4": "m4a",
    "application/zip": "zip",
}

def mime_type(path: Path) -> Optional[str]:
    if which("file"):
        rc, out, err = run(["file","-b","--mime-type", str(path)], timeout=10)
        if rc == 0:
            return out.strip().lower()
    return None

def detect_magic_family(path: Path, mode: str) -> Tuple[str, str, Optional[str]]:
    ext = path.suffix.lower().lstrip(".")
    if ext in EXT_FAMILY:
        fam, sub = EXT_FAMILY[ext]
        return fam, sub, None
    mt = None
    if mode == "heavy":
        mt = mime_type(path)
        if mt:
            if mt.startswith("image/"):
                subtype = mt.split("/",1)[1].upper()
                return ("Images", subtype, mt)
            if mt.startswith("video/"):
                return ("Video", mt.split("/",1)[1].upper(), mt)
            if mt.startswith("audio/"):
                return ("Audio", mt.split("/",1)[1].upper(), mt)
            if mt == "application/pdf":
                return ("PDFs","PDF", mt)
            if "postscript" in mt:
                return ("Adobe","EPS", mt)
            if "vnd.adobe.photoshop" in mt:
                return ("Adobe","Photoshop", mt)
            if "zip" in mt:
                return ("Archives","ZIP", mt)
            if "rtf" in mt:
                return ("Text","RTF", mt)
            if "plain" in mt:
                return ("Text","TXT", mt)
    return ("Other", ext.upper() if ext else "UNKNOWN", mt)

def ext_from_mime(mt: Optional[str]) -> Optional[str]:
    if not mt:
        return None
    return MIME_EXT.get(mt)
