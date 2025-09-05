# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Chris Ferebee
"""
cli.py

Command-line interface for the beachcomb carved file analysis and sorting tool.
"""

import argparse
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

from .core import Planner
from . import __version__
from .image_processing import ImgCfg
from .utils import parse_iso_datetime, log, check_dependencies
# Import the report generator
from .report_generator import generate_report


def main():
    # --- Fix for --version bug: Check for --version before parsing other args ---
    if "--version" in sys.argv:
        print(f"beachcomb {__version__}")
        sys.exit(0)

    start_time = time.time()
    
    ap = argparse.ArgumentParser(
        description="beachcomb: Analyze & sort carved files by filetype & date with dedup, integrity checks",
        epilog="Example: python3 cli.py --source ./recovered_files --dest ./sorted_output --undated-cutoff 2024-01-01T00:00:00 --commit"
    )
    ap.add_argument("--source", required=True, help="Source folder (carved files).")
    ap.add_argument("--dest", required=True, help="Destination root.")
    ap.add_argument(
        "--undated-cutoff",
        help=(
            "ISO date (YYYY-MM-DD); files with mtime newer than this are treated as undated "
            "and will try metadata date recovery. (default: 7 days ago)"
        ),
    )

    ap.add_argument("--max-per-bin", type=int, default=1000, help="Maximum files per date bin (default 1000).")
    ap.add_argument("--mode", choices=["light","heavy"], default="heavy", help="Light (quick mode) or heavy (default, deeper checks).")
    ap.add_argument("--workers", type=int, default=8, help="Parallel workers (default 8).")

    # PDFs
    ap.add_argument("--pdf-ocr", choices=["off","scans","all"], default="off", help="OCR policy for PDFs (default off).")
    ap.add_argument("--pdf-ocr-lang", default="eng", help="OCR language(s), e.g., 'eng', 'deu', or 'deu+eng'.")
    ap.add_argument("--pdf-ocr-workers", type=int, default=8, help="Concurrent OCR jobs (default 8).")
    ap.add_argument("--pdf-repair", choices=["off","on"], default="off", help="Try qpdf repair for damaged PDFs (heavy mode only).")

    # Office / Video extras
    ap.add_argument("--office-deep", choices=["on","off"], default=None, help="Deep integrity checks for Office/Numbers (default: on in heavy, off in light).")
    ap.add_argument("--video-repair", choices=["on","off"], default="off", help="Attempt ffmpeg rewrap for damaged videos (default off).")
    ap.add_argument("--video-decode-smoke", choices=["on","off"], default=None, help="Decode 1s smoke test for videos (default: off in light, on in heavy).")

    # Promotion & images
    ap.add_argument("--promote-threshold", type=int, default=20, help="Promote uncommon filetypes to their own top-level if at least this many exist (default 20).")
    ap.add_argument("--preview-short-side", type=int, default=700, help="Shorter edge (pixels) below which a JPEG may be called a preview (default 700).")
    ap.add_argument("--preview-max-mp", type=float, default=1.0, help="Megapixel cap for preview classification (default 1.0).")
    ap.add_argument("--preview-jpeg-only", type=int, default=1, help="Limit preview rule to JPEG only (1=true, 0=false).")
    ap.add_argument("--preview-ignore-exif", type=int, default=1, help="If 1, do NOT mark preview when EXIF Make/Model exists (default 1).")
    ap.add_argument("--ui-icon-sizes", default="16,32,64,128,256,512", help="Comma list of square icon sizes treated as UI-cache (default excludes 1024).")
    ap.add_argument("--screenshots", choices=["on","off"], default="on", help="Detect PNG screenshots and route to Images/_screenshots (default on).")
    ap.add_argument("--screenshot-tolerance-px", type=int, default=2, help="Pixel tolerance when matching known screen sizes (default 2).")
    ap.add_argument("--promote-make-threshold", type=int, default=100, help="Min count to promote a camera make (default 100).")
    ap.add_argument("--promote-model-threshold", type=int, default=100, help="Min count to promote a camera model (default 100).")

    # misc
    ap.add_argument("--version", action="store_true", help="Print version and exit.")
    ap.add_argument("--dry-run", action="store_true", help="Plan only; write manifest/report; do not move files.")
    ap.add_argument("--commit", action="store_true", help="Actually copy/move files into destination and set FS times when available.")
    ap.add_argument("--move", action="store_true", help="Move instead of copy (dangerous, untested, currently disabled!)")
    ap.add_argument("--rename", choices=["none","all","photorec"], default="photorec", help="Content-aware file renaming policy (default photorec = rename files named f1234567.type).")
    args = ap.parse_args()
    
    ## set default undated_cutoff to 7 days ago - assume no carved file has a younger original mtime
    if not args.undated_cutoff:
        args.undated_cutoff = (datetime.now() - timedelta(days=7)).date().isoformat()
        log(f"NOTE: --undated-cutoff not specified. Files newer than {args.undated_cutoff} will be treated as undated.")

    # --- Dependency Check ---
    # Check for tools required based on user-selected options.
    core_deps = ["exiftool", "ffprobe"]
    heavy_deps = ["pdfinfo", "pdftotext", "qpdf", "mutool", "gs", "b3sum"]

    deps_to_check = core_deps[:]
    if args.mode == "heavy":
        deps_to_check.extend(heavy_deps)
    if args.pdf_ocr != "off":
        deps_to_check.append("ocrmypdf")
    if args.video_repair == "on":
        deps_to_check.append("ffmpeg")
    if args.rename != "none":
        deps_to_check.extend(["exiftool", "zipinfo"])
        

    check_dependencies(list(set(deps_to_check)))

    # default toggles based on mode
    office_deep = args.office_deep
    if office_deep is None:
        office_deep = "on" if args.mode == "heavy" else "off"

    video_decode_smoke = args.video_decode_smoke
    if video_decode_smoke is None:
        # --- Fix for --video-decode-smoke default ---
        video_decode_smoke = "on" if args.mode == "heavy" else "off"

    source = Path(args.source).expanduser()
    dest = Path(args.dest).expanduser()
    cut = parse_iso_datetime(args.undated_cutoff)

    img_cfg = ImgCfg(args)
    
    log(f"beachcomb {__version__} starting up...")

    if args.move:
        log(f"DANGER -- Move mode is completely untested. Aborting!")
        sys.exit(1)

    planner = Planner(source, dest, cut, args.max_per_bin, args.mode, args.workers,
                      args.dry_run or (not args.commit), args.commit, args.move,
                      args.pdf_ocr, args.pdf_ocr_lang, args.pdf_ocr_workers,
                      args.promote_threshold, args.promote_make_threshold, args.promote_model_threshold,
                      img_cfg, args.pdf_repair, args.rename,
                      office_deep == "on", args.video_repair, video_decode_smoke == "on")
    planner.run()
    
    end_time = time.time()
    log("Processing complete. Generating final report...")
    
    tool_versions = {
        "exiftool": "00.00", 
        "qpdf": "00.002", 
        "ocrmypdf": "00.00"
    }
    
    generate_report(
        records=planner.records,
        output_path=dest / "recovery_report.html",
        is_dry_run=args.dry_run or (not args.commit),
        source_path=str(source),
        dest_path=str(dest),
        run_mode=args.mode,
        num_workers=args.workers,
        run_time_secs=end_time - start_time,
        tool_versions=tool_versions,
    )
    
    log("beachcomb run complete.")

if __name__ == "__main__":
    main()
