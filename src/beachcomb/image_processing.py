# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Chris Ferebee
"""
image_processing.py

Image-specific helpers and classification logic for the beachcomb tool.
"""
import re
from pathlib import Path
from typing import Optional, Tuple

from .utils import which, run
from .exiftoold import exiftool as et_call, available as et_available

class ImgCfg:
    def __init__(self, args):
        self.preview_short_side = args.preview_short_side
        self.preview_max_mp = args.preview_max_mp
        self.preview_jpeg_only = bool(args.preview_jpeg_only)
        self.preview_ignore_exif = bool(args.preview_ignore_exif)
        self.ui_icon_sizes = set(int(x) for x in args.ui_icon_sizes.split(",") if x.strip().isdigit())
        self.ui_small_size_bytes = 50 * 1024
        self.ui_small_alpha_bytes = 150 * 1024
        self.screenshots = (args.screenshots == "on")
        self.screenshot_tol = args.screenshot_tolerance_px
        self.screenshot_sizes = [
            (1136,640),(1334,750),(2208,1242),(2436,1125),(2532,1170),(2688,1242),(2778,1284),(2796,1290),
            (2556,1179),(1206,2622),
            (2048,1536),(2732,2048),
            (2560,1600),(2880,1800),(3024,1964),(3456,2234)
        ]

#def exif_make_model(path: Path) -> Tuple[str,str,str]:
#    if not et_available():
#        return "","",""
#    tags = ["-Make","-Model","-Software","-s","-s","-s", str(path)]
#    rc, out, _ = et_call(tags, timeout=10)
#    if rc != 0:
#        return "","",""
#    lines = [l.strip() for l in out.splitlines()]
#    vals = lines + ["","",""]
#    make = vals[0] if len(vals)>0 else ""
#    model = vals[1] if len(vals)>1 else ""
#    software = vals[2] if len(vals)>2 else ""
#    return make, model, software

def exif_make_model(path: Path) -> Tuple[str, str, str]:
    """
    Return (Make, Model, Software) as plain strings.
    Handles exiftoold's leading "{readyN}" correlation tokens.
    """
    if not et_available():
        return "", "", ""
    # Put formatting flags *before* tags; some exiftool builds care about order.
    args = ["-s", "-s", "-s", "-Make", "-Model", "-Software", str(path)]
    rc, out, _ = et_call(args, timeout=10)
    if rc != 0:
        return "", "", ""
    lines = [l.strip() for l in out.splitlines() if l.strip()]
    # Drop any leading "{readyN}" token from exiftoold
    while lines and re.fullmatch(r"\{ready\d+\}", lines[0]):
        lines.pop(0)
    # Pad to at least 3 so indexing is safe
    lines += ["", "", ""]
    make, model, software = lines[0], lines[1], lines[2]
    return make, model, software


def image_dimensions(path: Path) -> Tuple[Optional[int], Optional[int]]:
    if which("sips"):
        rc, out, _ = run(["sips","-g","pixelWidth","-g","pixelHeight", str(path)], timeout=10)
        if rc == 0:
            w = h = None
            for line in out.splitlines():
                if "pixelWidth:" in line:
                    try: w = int(line.split(":")[1].strip())
                    except: pass
                if "pixelHeight:" in line:
                    try: h = int(line.split(":")[1].strip())
                    except: pass
            if w and h:
                return w, h
    if et_available():
        rc, out, _ = et_call(["-s","-s","-s","-ImageWidth","-ImageHeight", str(path)], timeout=10)
        if rc == 0:
            lines = [l.strip() for l in out.splitlines()]
            try:
                w = int(lines[0]); h = int(lines[1])
                return w, h
            except Exception:
                pass
    return None, None

def sanitize_token(s: str) -> str:
    s = s.strip()
    s = re.sub(r"[^\w\-\.+]", "_", s)
    s = re.sub(r"_+", "_", s)
    return s

def is_iphone_photo_from_make_model(make: str, model: str) -> bool:
    m = (make or "").lower()
    mo = (model or "").lower()
    return ("apple" in m) and ("iphone" in mo)

def png_has_alpha(path: Path) -> bool:
    if which("file"):
        rc, out, _ = run(["file","-b", str(path)], timeout=5)
        if rc == 0 and "RGBA" in out:
            return True
    if et_available():
        rc, out, _ = et_call(["-s","-s","-s","-ColorType", str(path)], timeout=5)
        if rc == 0 and "Alpha" in out:
            return True
    return False

def detect_image_kind_judgement(ext: str, w: Optional[int], h: Optional[int], size_bytes: int,
                                has_alpha: bool, make: str, model: str,
                                cfg: ImgCfg) -> str:
    ext_l = ext.lower()
    short_side = min(w, h) if (w and h) else None
    mp = (w*h/1_000_000.0) if (w and h) else None
    make_l = (make or "").lower()
    model_l = (model or "").lower()

    if short_side and short_side in cfg.ui_icon_sizes and (w == h):
        return "ui-cache"
    if size_bytes < cfg.ui_small_size_bytes:
        return "ui-cache"
    if ext_l == "png" and has_alpha and size_bytes < cfg.ui_small_alpha_bytes:
        return "ui-cache"
    if short_side and short_side < 320 and ext_l in ("png","gif"):
        return "ui-cache"

    if cfg.screenshots and ext_l == "png":
        if not make_l and not model_l and w and h:
            for (sw, sh) in cfg.screenshot_sizes:
                if abs(w - sw) <= cfg.screenshot_tol or abs(h - sh) <= cfg.screenshot_tol or \
                   abs(w - sh) <= cfg.screenshot_tol or abs(h - sw) <= cfg.screenshot_tol:
                    return "screenshot"

    if cfg.preview_jpeg_only and ext_l not in ("jpg","jpeg"):
        return "normal"
    if short_side is not None and mp is not None:
        if short_side < cfg.preview_short_side and mp < cfg.preview_max_mp:
            if cfg.preview_ignore_exif and (make_l or model_l):
                return "normal"
            return "preview"

    return "normal"
