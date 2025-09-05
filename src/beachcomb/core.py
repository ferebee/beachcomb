# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Chris Ferebee
"""
beachcomb.py

Core logic for the beachcomb file recovery and sorting tool.
This file contains the Planner class which orchestrates the file processing,
validation, and sorting.
"""

import hashlib
import itertools
import os
import traceback
import sys
import shutil
import threading
import concurrent.futures as cf
import datetime as dt
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Local module imports
from . import utils
from . import hashing
from . import renaming
from . import type_detection
from . import date_recovery
from . import image_processing
from . import validation
from .exiftoold import exiftool as et_call, available as et_available


# ------------------------- main planner -------------------------

class Planner:
    def __init__(self, source: Path, dest: Path, undated_cutoff, max_per_bin: int, mode: str, workers: int,
                 dry_run: bool, commit: bool, move: bool, pdf_ocr: str, pdf_ocr_lang: str, pdf_ocr_workers: int,
                 promote_threshold: int, promote_make_threshold: int, promote_model_threshold: int,
                 img_cfg: image_processing.ImgCfg, pdf_repair: str, rename: str,
                 office_deep: bool, video_repair: str, video_decode_smoke: bool):
        self.source = source
        self.dest = dest
        self.undated_cutoff = undated_cutoff
        self.max_per_bin = max_per_bin
        self.mode = mode
        self.workers = workers
        self.dry_run = dry_run
        self.commit = commit
        self.move = move
        self.pdf_ocr = pdf_ocr
        self.pdf_ocr_lang = pdf_ocr_lang
        self.pdf_ocr_workers = pdf_ocr_workers
        self.promote_threshold = promote_threshold
        self.promote_make_threshold = promote_make_threshold
        self.promote_model_threshold = promote_model_threshold
        self.img_cfg = img_cfg
        self.pdf_repair = pdf_repair
        self.rename = rename
        self.office_deep = office_deep
        self.video_repair = video_repair
        self.video_decode_smoke = video_decode_smoke
        self.records = []
        self.apfs = utils.is_apfs(dest)

    def discover_files(self):
        paths = []
        for root, dirs, files in os.walk(self.source):
            for name in files:
                p = Path(root) / name
                try:
                    if p.is_file():
                        paths.append(p)
                except Exception:
                    continue
        return paths

    def process_file(self, path: Path):
        family, subtype, mime = type_detection.detect_magic_family(path, self.mode)
        mtime_local = utils.file_mtime(path)
        size = path.stat().st_size

        integrity, extra = validation.quick_integrity(path, family, subtype, self.mode,
                                                      self.office_deep, self.video_decode_smoke)

        if subtype == "ZIP" and extra.get("archive_subtype"):
            subtype = extra["archive_subtype"]

        if subtype == "ZIP" and extra.get("archive_subtype"):
            subtype = extra["archive_subtype"]

        undated_flag = 1 if mtime_local > self.undated_cutoff else 0

        date_source = None
        date_local = None
        try_exif = family in ("Images","Video") or (family=="PDFs")
        video_duration = extra.get("video_duration","")

        if undated_flag and try_exif:
            if family == "Images":
                date_source, date_local = date_recovery.exiftool_date(path)
 #               utils.log(f"date_source: {date_source}  ---  date_local: {date_local}  --- file: {path}")
            elif family == "Video":
                src1, dt1 = date_recovery.video_date(path)
                src2, dt2, dur = date_recovery.ffprobe_date_and_duration(path)
                if not video_duration and dur is not None:
                    video_duration = str(dur)
                now = dt.datetime.now(dt.timezone.utc).astimezone()
                earliest = dt.datetime(1995,1,1,tzinfo=now.tzinfo)
                latest   = now + dt.timedelta(days=30)
                candidates = []
                if dt1: candidates.append(("video_meta", dt1))
                if dt2: candidates.append(("ffprobe_creation_time", dt2))
                picked = None
                for label, d in candidates:
                    if earliest <= d <= latest:
                        picked = (label, d); break
                if picked and (dur is None or dur <= 0):
                    picked = None
                if picked:
                    date_source, date_local = picked
            elif family == "PDFs":
                date_source, date_local = date_recovery.pdfinfo_dates(path) #  <- This is the bug

        pdf_kind = ""
        pdf_version = pdf_encrypted = pdf_linearized = ""
        pdf_error = ""
        if family == "PDFs":
            kind = "Scans"
            if utils.which("pdffonts"):
                rc, out, _ = utils.run(["pdffonts", str(path)], timeout=20)
                if rc == 0 and out and len([l for l in out.splitlines() if l.strip()]) > 2:
                    kind = "Digital"
            if utils.which("pdftotext") and kind != "Digital":
                rc, out, _ = utils.run(["pdftotext","-f","1","-l","1","-nopgbrk","-q", str(path), "-"], timeout=20)
                if rc == 0 and (out and len(out.strip())>0):
                    kind = "Digital"
            pdf_kind = kind
            subtype = kind
            pv, pe, pl = validation.parse_pdfinfo_meta(path)
            pdf_version = pv or ""
            pdf_encrypted = pe or ""
            pdf_linearized = pl or ""
            pdf_error = extra.get("pdf_error", "")

        exif_mk = exif_md = exif_sw = ""
        px_w = px_h = None
        img_kind = ""
        iphone = False
        if family == "Images":
            px_w, px_h = image_processing.image_dimensions(path)
            if et_available():
                exif_mk, exif_md, exif_sw = image_processing.exif_make_model(path)
            if image_processing.is_iphone_photo_from_make_model(exif_mk, exif_md) and subtype in ("JPG","HEIC"):
                iphone = True
                subtype = f"iPhone-{subtype}"
            has_alpha = image_processing.png_has_alpha(path) if path.suffix.lower()==".png" else False
            img_kind = image_processing.detect_image_kind_judgement(path.suffix.lstrip("."), px_w, px_h, size,
                                                   has_alpha, exif_mk, exif_md, self.img_cfg)

        src_ext = path.suffix.lower().lstrip(".")
        guessed_ext = None
        if self.mode == "heavy":
            guessed_ext = type_detection.ext_from_mime(mime)

        type_label = ""
        if family == "Other":
            if guessed_ext:
                type_label = guessed_ext.upper()
            elif src_ext:
                type_label = src_ext.upper()
            else:
                type_label = subtype or "UNKNOWN"

        size_bytes, head, tail = utils.read_head_tail_signature(path, block_size=256*1024 if self.mode=="light" else 1024*1024)
        sig = f"{size_bytes}:{hashing.fast_hash(head)}:{hashing.fast_hash(tail)}"

        rec = {
            "source_path": str(path),
            "family": family,
            "subtype": subtype,
            "ext": src_ext,
            "size_bytes": size,
            "mtime_local": mtime_local.isoformat(),
            "integrity": integrity,
            "undated_flag": undated_flag,
            "date_source": date_source or "",
            "date_local": date_local.isoformat() if date_local else "",
            "sig": sig,
            "fullhash": "",
            "duplicate_of": "",
            "dest_path": "",
            "pdf_kind": pdf_kind,
            "iphone": 1 if iphone else 0,
            "mime": mime or "",
            "guessed_ext": guessed_ext or "",
            "type_label": type_label,
            "img_kind": img_kind,
            "exif_make": exif_mk,
            "exif_model": exif_md,
            "exif_software": exif_sw,
            "px_w": px_w or "",
            "px_h": px_h or "",
            "pdf_version": pdf_version,
            "pdf_encrypted": pdf_encrypted,
            "pdf_linearized": pdf_linearized,
            "pdf_error": pdf_error,
            "pdf_ocr_applied": 0,
            "office_error": extra.get("office_error",""),
            "video_duration": video_duration,
            "video_repaired": 0,
            "video_error_source": "",
        }
        return rec

    def stage_full_hash_for_collisions(self):
        buckets = {}
        for idx, r in enumerate(self.records):
            buckets.setdefault(r["sig"], []).append(idx)
        multi = [idxs for idxs in buckets.values() if len(idxs) > 1]
        if not multi:
            return
        use_b3 = utils.which("b3sum") is not None and self.mode == "heavy"

        def do_hash(idx: int):
            path = Path(self.records[idx]["source_path"])
            h = None
            if use_b3:
                h = hashing.full_hash_b3sum(path)
            if not h:
                h = hashing.full_hash_openssl(path, algo="sha256")
            if not h:
                BUF=1024*1024
                sha = hashlib.sha256()
                with open(path,"rb") as f:
                    for chunk in iter(lambda: f.read(BUF), b""):
                        sha.update(chunk)
                h = sha.hexdigest()
            self.records[idx]["fullhash"] = h

        with cf.ThreadPoolExecutor(max_workers=max(1, self.workers//2)) as ex:
            list(ex.map(do_hash, itertools.chain.from_iterable(multi)))

        for idxs in multi:
            by_full = {}
            for i in idxs:
                fh = self.records[i]["fullhash"]
                if not fh: 
                    continue
                by_full.setdefault(fh, []).append(i)
            for fh, group in by_full.items():
                keeper = min(group)
                for j in group:
                    if j != keeper:
                        self.records[j]["duplicate_of"] = self.records[keeper]["source_path"]

    def promote_other_types(self):
        counts = {}
        for r in self.records:
            if r["family"] == "Other" and r["type_label"]:
                counts[r["type_label"]] = counts.get(r["type_label"], 0) + 1
        for r in self.records:
            if r["family"] == "Other" and r["type_label"]:
                if counts[r["type_label"]] >= self.promote_threshold:
                    r["family"] = r["type_label"]
                    r["subtype"] = r["type_label"]

    def collect_make_model_counts(self):
        make_counts = {}
        model_counts = {}
        for r in self.records:
            if r["family"] != "Images":
                continue
            if r.get("img_kind") in ("ui-cache","preview","screenshot"):
                continue
            st = r.get("subtype","") or ""
            if st.startswith("iPhone-"):
                continue
            mk = (r.get("exif_make") or "").strip()
            md = (r.get("exif_model") or "").strip()
            if mk:
                make_counts[mk] = make_counts.get(mk, 0) + 1
            if mk and md:
                key = (mk, md)
                model_counts[key] = model_counts.get(key, 0) + 1
        return make_counts, model_counts

    # ------------------------- binning -------------------------

    def _dest_filename(self, r: dict) -> str:
        src = Path(r["source_path"])
        guessed_ext = r.get("guessed_ext") or None
        
        if self.rename != "none":
            new_name = renaming.generate_new_name(src, self.rename, r)
            if new_name:
                return new_name

        src_ext = src.suffix.lower().lstrip(".")
        name = src.name
        if (not src_ext) and guessed_ext:
            if not name.endswith("." + guessed_ext):
                name = f"{name}.{guessed_ext}"
        return name

    def _assign_dest(self, idx: int, bin_dir: Path):
        r = self.records[idx]
        dest_name = self._dest_filename(r)
        candidate = bin_dir / dest_name
        candidate = utils.ensure_unique_path(candidate)
        self.records[idx]["dest_path"] = str(candidate)

    def _greedy_merge(self, keys_sorted: List, counts: Dict, max_per_bin: int):
        bins = []
        if not keys_sorted:
            return bins
        start = 0
        while start < len(keys_sorted):
            total = 0
            end = start
            while end < len(keys_sorted) and total + counts[keys_sorted[end]] <= max_per_bin:
                total += counts[keys_sorted[end]]
                end += 1
            bins.append((start, end))
            start = end
        return bins

    def _label_year_range(self, y1: int, y2: int) -> str:
        return f"{y1}" if y1 == y2 else f"{y1}-{y2}"

    def _label_month_range(self, y: int, m1: int, m2: int) -> str:
        if m1 == m2:
            return f"{y:04d}-{m1:02d}"
        return f"{y:04d}-{m1:02d}-{y:04d}-{m2:02d}"

    def _label_day_range(self, y: int, m: int, d1: int, d2: int) -> str:
        if d1 == d2:
            return f"{y:04d}-{m:02d}-{d1:02d}"
        return f"{y:04d}-{m:02d}-{d1:02d}-{y:04d}-{m:02d}-{d2:02d}"

    def _label_minute_range(self, y: int, m: int, d: int, h1: int, mi1: int, h2: int, mi2: int) -> str:
        lhs = f"{y:04d}-{m:02d}-{d:02d}_{h1:02d}{mi1:02d}"
        if h1 == h2 and mi1 == mi2:
            return lhs
        rhs = f"{y:04d}-{m:02d}-{d:02d}_{h2:02d}{mi2:02d}"
        return f"{lhs}-{rhs}"

    def plan_bins(self):
        self.promote_other_types()

        make_counts, model_counts = self.collect_make_model_counts()
# don't skip gating logic        
        promoted_makes = {mk for mk, c in make_counts.items() if c >= self.promote_make_threshold}
        promoted_models = {mm for mm, c in model_counts.items() if c >= self.promote_model_threshold}

        for r in self.records:
            if r["family"] != "Images":
                continue
            if r["integrity"] != "ok":
                continue
            if r.get("img_kind") == "ui-cache":
                r["subtype"] = "_ui-cache"; continue
            if r.get("img_kind") == "screenshot":
                r["subtype"] = "_screenshots"; continue
            if r.get("img_kind") == "preview":
                r["subtype"] = "_previews"; continue
            st = r.get("subtype","")
            if st.startswith("iPhone-"):
                continue
            mk = (r.get("exif_make") or "").strip()
            md = (r.get("exif_model") or "").strip()
# conditionally sort by gated make and model types            
            if mk and md and (mk, md) in promoted_models:
                r["subtype"] = f"Camera-Model/{image_processing.sanitize_token(mk)}_{image_processing.sanitize_token(md)}"; continue
            if mk and mk in promoted_makes:
                r["subtype"] = f"Camera-Make/{image_processing.sanitize_token(mk)}"; continue
# or Unconditionally sort by camera model when available, else by make.
#            if mk and md:
#                r["subtype"] = (
#                    f"Camera-Model/{image_processing.sanitize_token(mk)}_"
#                    f"{image_processing.sanitize_token(md)}"
#                )
#                continue
#            if mk:
#                r["subtype"] = f"Camera-Make/{image_processing.sanitize_token(mk)}"
#                continue
# end unconditional mod

        groups: Dict[Tuple[str,str], List[int]] = {}
        for idx, r in enumerate(self.records):
            if r["integrity"] != "ok":
                continue
            key = (r["family"], r["subtype"])
            groups.setdefault(key, []).append(idx)

        base = self.dest

        for (family, subtype), idxs in groups.items():
            fam_root = base / family / subtype

            dated: List[Tuple[int, dt.datetime]] = []
            unknown: List[int] = []

            for i in idxs:
                r = self.records[i]
                ts = None
                if r["date_local"]:
                    try:
                        ts = dt.datetime.fromisoformat(r["date_local"])
                    except Exception:
                        ts = None
                else:
                    if int(r["undated_flag"]) == 0:
                        try:
                            ts = dt.datetime.fromisoformat(r["mtime_local"])
                            self.records[i]["date_source"] = self.records[i]["date_source"] or "mtime"
                            self.records[i]["date_local"] = self.records[i]["date_local"] or r["mtime_local"]
                        except Exception:
                            ts = None
                if ts is None:
                    unknown.append(i)
                else:
                    dated.append((i, ts))

            dated.sort(key=lambda x: x[1])
            unknown.sort(key=lambda j: self.records[j]["source_path"])

            years: Dict[int, List[int]] = {}
            months: Dict[Tuple[int,int], List[int]] = {}
            days: Dict[Tuple[int,int,int], List[int]] = {}
            minutes: Dict[Tuple[int,int,int,int,int], List[int]] = {}

            for i, ts in dated:
                y = ts.year; m = ts.month; d = ts.day; h = ts.hour; mi = ts.minute
                years.setdefault(y, []).append(i)
                months.setdefault((y,m), []).append(i)
                days.setdefault((y,m,d), []).append(i)
                minutes.setdefault((y,m,d,h,mi), []).append(i)

            year_keys = sorted(years.keys())
            heavy_years = set([y for y in year_keys if len(years[y]) > self.max_per_bin])
            bins_plan: List[Tuple[str, List[int]]] = []

            run = []
            for y in year_keys:
                if y in heavy_years:
                    if run:
                        k_sorted = run[:]
                        counts = {yy: len(years[yy]) for yy in k_sorted}
                        spans = self._greedy_merge(k_sorted, counts, self.max_per_bin)
                        for s,e in spans:
                            yy1 = k_sorted[s]; yy2 = k_sorted[e-1]
                            label = self._label_year_range(yy1, yy2)
                            idxs_in_bin = list(itertools.chain.from_iterable(years[yy] for yy in k_sorted[s:e]))
                            bins_plan.append((label, idxs_in_bin))
                        run = []
                else:
                    run.append(y)
            if run:
                k_sorted = run[:]
                counts = {yy: len(years[yy]) for yy in k_sorted}
                spans = self._greedy_merge(k_sorted, counts, self.max_per_bin)
                for s,e in spans:
                    yy1 = k_sorted[s]; yy2 = k_sorted[e-1]
                    label = self._label_year_range(yy1, yy2)
                    idxs_in_bin = list(itertools.chain.from_iterable(years[yy] for yy in k_sorted[s:e]))
                    bins_plan.append((label, idxs_in_bin))

            for y in sorted(heavy_years):
                month_keys = sorted([k for k in months.keys() if k[0] == y])
                counts = {k: len(months[k]) for k in month_keys}
                heavy_months = set([k for k in month_keys if counts[k] > self.max_per_bin])

                segment = []
                def flush_segment(seg):
                    if not seg: return
                    seg_counts = {k: counts[k] for k in seg}
                    spans = self._greedy_merge(seg, seg_counts, self.max_per_bin)
                    for s,e in spans:
                        y1,m1 = seg[s]; y2,m2 = seg[e-1]
                        label = self._label_month_range(y, m1, m2)
                        idxs_in_bin = list(itertools.chain.from_iterable(months[k] for k in seg[s:e]))
                        bins_plan.append((label, idxs_in_bin))

                for k in month_keys:
                    if k in heavy_months:
                        flush_segment(segment); segment = []
                        y_m = k
                        day_keys = sorted([kk for kk in days.keys() if kk[0]==y_m[0] and kk[1]==y_m[1]])
                        day_counts = {kk: len(days[kk]) for kk in day_keys}
                        heavy_days = set([kk for kk in day_keys if day_counts[kk] > self.max_per_bin])

                        seg_d = []
                        def flush_day_segment(segd):
                            if not segd: return
                            seg_ct = {kk: day_counts[kk] for kk in segd}
                            spans_d = self._greedy_merge(segd, seg_ct, self.max_per_bin)
                            for sd,ed in spans_d:
                                y2,m2,d1 = segd[sd]; y3,m3,d2 = segd[ed-1]
                                label = self._label_day_range(y2, m2, d1, d2)
                                idxs_in_bin = list(itertools.chain.from_iterable(days[kk] for kk in segd[sd:ed]))
                                bins_plan.append((label, idxs_in_bin))

                        for kk in day_keys:
                            if kk in heavy_days:
                                flush_day_segment(seg_d); seg_d = []
                                y4,m4,d4 = kk
                                minute_keys = sorted([mmmm for mmmm in minutes.keys() if mmmm[0]==y4 and mmmm[1]==m4 and mmmm[2]==d4])
                                minute_counts = {mm: len(minutes[mm]) for mm in minute_keys}
                                spans_m = self._greedy_merge(minute_keys, minute_counts, self.max_per_bin)
                                for sm,em in spans_m:
                                    h1, mi1 = minute_keys[sm][3], minute_keys[sm][4]
                                    h2, mi2 = minute_keys[em-1][3], minute_keys[em-1][4]
                                    label = self._label_minute_range(y4, m4, d4, h1, mi1, h2, mi2)
                                    idxs_in_bin = list(itertools.chain.from_iterable(minutes[mm] for mm in minute_keys[sm:em]))
                                    if len(idxs_in_bin) > self.max_per_bin:
                                        idxs_sorted = sorted(idxs_in_bin, key=lambda i: self.records[i]["source_path"])
                                        parts = [idxs_sorted[x:x+self.max_per_bin] for x in range(0, len(idxs_in_bin), self.max_per_bin)]
                                        for pi, part in enumerate(parts, start=1):
                                            bins_plan.append((f"{label}-{pi:03d}", part))
                                    else:
                                        bins_plan.append((label, idxs_in_bin))
                            else:
                                seg_d.append(kk)
                        flush_day_segment(seg_d)
                    else:
                        segment.append(k)
                flush_segment(segment)

            if unknown:
                for start in range(0, len(unknown), self.max_per_bin):
                    part = unknown[start:start+self.max_per_bin]
                    label = f"undated-{(start//self.max_per_bin)+1:04d}"
                    bins_plan.append((label, part))
            
            # Assign destinations (reads metadata via shared exiftoold)
            for label, members in sorted(bins_plan, key=lambda t: t[0]):
                bin_dir = fam_root / label
                for i in sorted(members, key=lambda j: self.records[j]["source_path"]):
                    self._assign_dest(i, bin_dir)
            

    def write_manifest_csv(self, out_path: Path):
        fields = ["source_path","dest_path","family","subtype","ext","size_bytes","mtime_local","integrity","undated_flag","date_source","date_local","sig","fullhash","duplicate_of","pdf_kind","iphone","mime","guessed_ext","type_label","img_kind","exif_make","exif_model","exif_software","px_w","px_h","pdf_version","pdf_encrypted","pdf_linearized","pdf_error","pdf_ocr_applied","office_error","video_duration","video_repaired","video_error_source"]
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            import csv as _csv
            w = _csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            for r in self.records:
                w.writerow({k:r.get(k,"") for k in fields})

    def _copy_or_move(self, src: Path, dest: Path):
        if self.move:
            if not dest.exists():
                shutil.move(str(src), str(dest))
        else:
            if not dest.exists():
                if utils.is_apfs(Path(dest).parent):
                    rc = os.system(f"/bin/cp -c '{src}' '{dest}' > /dev/null 2>&1")
                    if rc != 0:
                        shutil.copy2(str(src), str(dest))
                else:
                    shutil.copy2(str(src), str(dest))

    def commit_moves(self):
        if not self.commit:
            return

        # Ensure target dirs exist
        for r in self.records:
            dest = None
            if r["integrity"] != "ok":
                dest = self.dest / "damaged" / r["family"] / r["subtype"] / Path(r["source_path"]).name
            else:
                if r["dest_path"]:
                    dest = Path(r["dest_path"])
            if dest:
                dest.parent.mkdir(parents=True, exist_ok=True)

        # Attempt PDF repair (heavy only)
        for r in self.records:
            if r["family"] != "PDFs": continue
            if r["integrity"] == "ok": continue
            if self.pdf_repair != "on" or self.mode != "heavy": continue
            src = Path(r["source_path"])
            target = Path(r["dest_path"]) if r["dest_path"] else (self.dest / "PDFs" / "_repaired" / src.name)
            repaired_ok = validation.pdf_try_repair(src, target)
            if repaired_ok:
                integ, _ = validation.quick_integrity(target, "PDFs", r.get("pdf_kind") or "PDF", self.mode, self.office_deep, self.video_decode_smoke)
                if integ == "ok":
                    r["integrity"] = "ok"
                    r["dest_path"] = str(target)
                    continue

        # Attempt VIDEO repair when requested
        if self.video_repair == "on":
            for r in self.records:
                if r["family"] != "Video": continue
                if r["integrity"] == "ok": continue
                src = Path(r["source_path"])
                target = Path(r["dest_path"]) if r["dest_path"] else (self.dest / "Video" / "_repaired" / src.with_suffix(".mp4").name)
                if validation.try_ffmpeg_rewrap(src, target):
                    integ, extra = validation.quick_integrity(target, "Video", r.get("subtype",""), self.mode, self.office_deep, self.video_decode_smoke)
                    if integ == "ok":
                        r["integrity"] = "ok"
                        r["dest_path"] = str(target)
                        r["video_repaired"] = 1
                        r["video_error_source"] = r.get("integrity","")
                        continue

        # Initial copy (skip OCR-candidates until OCR stage)
        for r in self.records:
            if r["duplicate_of"]:
                continue
            src = Path(r["source_path"])
            if r["integrity"] != "ok":
                dest = self.dest / "damaged" / r["family"] / r["subtype"] / src.name
                self._copy_or_move(src, dest); continue
            dest = Path(r["dest_path"]) if r["dest_path"] else None
            if not dest: continue
            if (self.pdf_ocr != "off") and (r["family"] == "PDFs"):
                is_candidate = (self.pdf_ocr == "all") or (self.pdf_ocr == "scans" and (r.get("pdf_kind") == "Scans"))
                if is_candidate:
                    continue
            self._copy_or_move(src, dest)

        # OCR stage
        ocr_candidates = []
        if self.pdf_ocr != "off":
            for r in self.records:
                if r["integrity"] != "ok" or r["duplicate_of"]:
                    continue
                if r["family"] == "PDFs":
                    if self.pdf_ocr == "all" or (self.pdf_ocr == "scans" and r.get("pdf_kind") == "Scans"):
                        ocr_candidates.append(r)

        def do_ocr(r):
            src = Path(r["source_path"])
            dest = Path(r["dest_path"]) if r["dest_path"] else None
            if not dest:
                return False
            ok = validation.ocr_pdf(src, dest, self.pdf_ocr_lang)
            if ok:
                r["pdf_ocr_applied"] = 1
            if not ok:
                self._copy_or_move(src, dest)
            return ok

        if ocr_candidates and utils.which("ocrmypdf"):
            utils.log(f"OCR: starting {len(ocr_candidates)} PDFs with up to {self.pdf_ocr_workers} workers")
            with cf.ThreadPoolExecutor(max_workers=self.pdf_ocr_workers) as ex:
                list(ex.map(do_ocr, ocr_candidates))

        # Set FS times when we have a recovered date
        for r in self.records:
            if not r["dest_path"]:
                continue
            dest = Path(r["dest_path"])
            if not dest.exists():
                continue
            if r["date_local"]:
                try:
                    if et_available():
                        val = r["date_local"].replace("T", " ")
                        et_call(["-overwrite_original",
                             f"-FileModifyDate={val}", f"-FileCreateDate={val}", str(dest)], timeout=20)
                    else:
                        ts = dt.datetime.fromisoformat(r["date_local"]).timestamp()
                        os.utime(dest, times=(ts, ts))
                except Exception as e:
                    # (Optional) log the error; avoid swallowing silently
                    utils.log(f"Failed to set times for {dest}: {e}")
                    pass

    def run(self):
        utils.log("Discovering files...")
        files = self.discover_files()
        utils.log(f"Found {len(files)} files")

        records = []
        lock = threading.Lock()
        failures = []
        fail_open = os.environ.get("BC_FAIL_OPEN", "0") not in ("0","false","False","no","NO")
        trace = os.environ.get("BC_TRACE", "0") not in ("0","false","False","no","NO")
 

        def worker(p: Path):
            try:
                rec = self.process_file(p)
                with lock:
                    records.append(rec)
            except Exception as e:
                msg = f"ERROR processing file: {p} :: {e.__class__.__name__}: {e}"
                if trace:
                    tb = traceback.format_exc(limit=6)
                    utils.log(msg + "\n" + tb)
                else:
                    utils.log(msg)
                with lock:
                    failures.append((p, str(e)))
                # Fail-open: emit a minimal record so copying can proceed if desired
                if fail_open:
                    try:
                        mtime_local = utils.file_mtime(p)
                        rec = {
                            "source_path": str(p),
                            "family": "Other",
                            "subtype": "Unknown",
                            "mime": None,
                            "mtime_local": mtime_local,
                            "size": p.stat().st_size,
                            "duplicate_of": None,
                            "integrity": "unknown",
                            "notes": "fail-open",
                        }
                        with lock:
                            records.append(rec)
                    except Exception:
                        # If even fail-open fails, we just skip this file.
                        if trace:
                            utils.log("Fail-open also failed:\n" + traceback.format_exc(limit=4))

        with cf.ThreadPoolExecutor(max_workers=self.workers) as ex:
            list(ex.map(worker, files))

        self.records = records
        total_files = len(files)
        if failures:
            utils.log(f"Processing failures: {len(failures)}/{total_files}. "
                      f"Set BC_TRACE=1 for tracebacks, BC_FAIL_OPEN=1 to keep going with minimal records.")
            # Show up to 5 examples
            for p, err in failures[:5]:
                utils.log(f"  · {p} → {err}")

        utils.log("Planning dedup full-hash confirmations...")
        self.stage_full_hash_for_collisions()
        
        utils.log("Planning bins...")
        self.plan_bins()
        
        # --- diagnostics: what will be copied? ---
        total = len(self.records)
        with_dest = sum(1 for r in self.records if r.get("dest_path"))
        dups = sum(1 for r in self.records if r.get("duplicate_of"))
        damaged = sum(1 for r in self.records if r.get("integrity") != "ok")
        ocr_wait = sum(
            1 for r in self.records
            if r.get("family") == "PDFs"
            and (self.pdf_ocr != "off")
            and (self.pdf_ocr == "all" or (self.pdf_ocr == "scans" and r.get("pdf_kind") == "Scans"))
        )
        utils.log(f"Plan summary: total={total}, with_dest={with_dest}, duplicates={dups}, damaged={damaged}, pdfs_for_ocr={ocr_wait}")

        manifests_dir = self.dest / "manifests"
        # reports_dir = self.dest / "reports"
        self.write_manifest_csv(manifests_dir / "manifest.csv")
        # self.report_func(reports_dir / "index.html", self)

        if self.commit:
            utils.log("Committing file copies/moves...")
            self.commit_moves()
        else:
            utils.log("DRY RUN complete (no files moved). Manifest and report written.")
