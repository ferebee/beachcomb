# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Chris Ferebee
"""
renaming.py

Content-aware file renaming for the beachcomb tool.
"""
import re
import subprocess
# from __future__ import annotations
import zipfile
import json, unicodedata
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional, Dict, List
from collections import Counter

from .utils import run, which, zip_list_contents, log
from .exiftoold import exiftool as et_call, available as et_available

def extract_metadata_title(path: Path) -> Optional[str]:
    """
    Extracts a title-like field from file metadata using a persistent ExifTool process.
    This version is prioritized for image metadata standards like IPTC and EXIF.
    """
    # First, try the direct OOXML parsing method for modern Office files.
    title = extract_ooxml_title(path)
    if title:
        return title

    # For other file types, use the running exiftool daemon.
    # Prioritized list of tags to check for a title or descriptive comment.
    tags = [
        # --- High-value, title-like fields (best for filenames) ---
        "-ObjectName",        # IPTC standard for a short, specific title.
        "-Headline",          # IPTC standard for a punchy headline.
        "-Title",             # Generic/XMP title.

        # --- Descriptive fields (can be longer but often useful) ---
        "-ImageDescription",  # The most common EXIF description tag.
        "-UserComment",       # EXIF tag for free-form user comments.
        "-Caption-Abstract",  # IPTC standard for a detailed caption.
        "-Description",       # Generic/XMP description.
        "-Label",             # XMP tag for a short, categorical label (e.g., from Adobe Bridge).
        
        # --- Keyword/Subject fields (good fallback) ---
        "-Keywords",
        "-Subject",
        
        # --- Creator fields (last resort) ---
        "-Author",
        "-Creator",
        "-By-line"            # IPTC standard for the author's name.
    ]
    
    # Ask shared exiftoold daemon (returns rc, stdout, stderr_like)
    rc, out, _ = et_call(tags + [str(path)], timeout=15)
    output_lines = out.splitlines() if rc == 0 and out else []

    if not output_lines:
        return None
        
    # Exiftool output is "TagName : Value". We need to split and get the value.
    for line in output_lines:
        if ':' in line:
            # Split only on the first colon to handle values that contain colons.
            _, value = line.split(':', 1)
            value = value.strip()
            if value:
                return value
    return None

def generate_new_name(path: Path, policy: str, record: Dict) -> Optional[str]:
    """Main dispatcher for generating a new filename based on policy."""
    if policy == "photorec" and not is_photorec_name(path.name):
        return None

    original_stem = path.stem
    original_suffix = path.suffix.lower()
    new_name_part = None

    if record.get('family') == 'Archives' and record.get('subtype', '').startswith('ZIP'):
        new_name_part = rename_zip_archive(path)
    else:
        # Pass the daemon instance to the metadata extraction function
        title = extract_metadata_title(path)
        if title:
            sanitized = sanitize_and_truncate(title)
            if sanitized:
                new_name_part = f"-{sanitized}"

    if new_name_part:
        candidate = f"{original_stem}{new_name_part}{original_suffix}"
        return enforce_filename_byte_limit(candidate)
    
    return None

def extract_ooxml_title(path: Path) -> Optional[str]:
    """Extracts the title from a modern Office file by reading core.xml."""
    if path.suffix.lower() not in (".docx", ".xlsx", ".pptx"):
        return None

    try:
        with zipfile.ZipFile(path, 'r') as zf:
            if "docProps/core.xml" in zf.namelist():
                with zf.open("docProps/core.xml") as core_xml:
                    tree = ET.parse(core_xml)
                    root = tree.getroot()
                    # Define the Dublin Core namespace to find the title tag
                    namespaces = {'dc': 'http://purl.org/dc/elements/1.1/'}
                    title_element = root.find('dc:title', namespaces)
                    if title_element is not None and title_element.text:
                        return title_element.text.strip()
    except (zipfile.BadZipFile, ET.ParseError, KeyError):
        # File may be corrupt, not a valid zip, or XML is malformed
        return None
    return None

def extract_xls_content_fallback(path: Path) -> Optional[str]:
    """As a last resort for .xls files, uses ssconvert to get cell A1."""
    if path.suffix.lower() != ".xls" or not which("ssconvert"):
        return None
    
    # Command to convert the xls to csv and pipe to stdout
    cmd = ["ssconvert", str(path), "fd://1", "-O", "separator=,"]
    rc, out, _ = run(cmd, timeout=20)

    if rc == 0 and out.strip():
        # Get the first column of the first row
        first_cell = out.splitlines()[0].split(',')[0].strip('"')
        if first_cell:
            return first_cell
    return None

def is_photorec_name(filename: str) -> bool:
    """Checks if a filename matches the PhotoRec f####### pattern."""
    return bool(re.match(r"f\d{7,}", Path(filename).stem))

def _clean_title_text(s: str) -> str:
    """Fix common metadata artifacts: embedded NULs, XP UTF-16LE chunks, normalize."""
    if not s:
        return s
    # Remove embedded NULs (UTF-16LE interpreted as Latin-1)
    s = s.replace("\x00", "")
    # Collapse patterns like "_000F_000w_000d" -> "Fwd"
    s = re.sub(r'(?:^|_)0{3}([A-Za-z0-9])', r'\1', s)
    # Normalize Unicode and strip odd edges
    s = unicodedata.normalize("NFC", s).strip(" -_.")
    return s

def enforce_filename_byte_limit(basename: str, max_bytes: int = 240) -> str:
    """
    Ensure basename (stem+suffix) fits within max_bytes (APFS limit is 255).
    Prefer trimming from the right (title part), keep suffix intact.
    """
    suffix = Path(basename).suffix
    stem = basename[:-len(suffix)] if suffix else basename
    while len(basename.encode("utf-8")) > max_bytes and len(stem) > 1:
        stem = stem[:-1]
        basename = stem + suffix
    return basename

def sanitize_and_truncate(text: str, max_len: int = 60) -> str:
    """Sanitize to filename-safe and cap characters (additional byte cap happens later)."""
    text = _clean_title_text(text)
    # Don’t treat text as a path; keep all characters then sanitize
    sanitized = re.sub(r'[^\w\-]+', '-', text)     # keep letters/digits/_/-
    sanitized = re.sub(r'--+', '-', sanitized).strip('-_ .')
    if len(sanitized) > max_len:
        if '-' in sanitized[:max_len]:
            return sanitized[:sanitized.rfind('-', 0, max_len)]
        return sanitized[:max_len]
    return sanitized

def rename_zip_archive(path: Path) -> Optional[str]:
    """Generates a new name for a ZIP archive based on its contents."""
    contents = zip_list_contents(path)
    if not contents:
        return None

    extensions = [Path(f).suffix.lower() for f in contents if Path(f).suffix and len(Path(f).suffix) > 1]
    if not extensions:
        return None
    
    most_common_ext_tuple = Counter(extensions).most_common(1)
    if not most_common_ext_tuple:
        return None
    most_common_ext = most_common_ext_tuple[0][0]
    count = len([ext for ext in extensions if ext == most_common_ext])

    first_file_of_type = next((f for f in contents if f.lower().endswith(most_common_ext)), None)
    if not first_file_of_type:
        return None

    sanitized_name = sanitize_and_truncate(first_file_of_type)
    if not sanitized_name:
        return None
    
    type_label = most_common_ext.lstrip('.').upper()
    
    return f"-{sanitized_name}+{count}-{type_label}"
