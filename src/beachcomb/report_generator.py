# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Chris Ferebee
"""
report_generator.py

Generates a detailed HTML report for a file recovery process.
This script incorporates detailed insights and a timeline of recovered files.
"""

import math
from pathlib import Path
from collections import Counter, defaultdict
from typing import Any, Dict, List, Optional
from datetime import datetime
from . import __version__


def _format_bytes(byte_count: int) -> str:
    """Formats a byte count into a human-readable string (e.g., KB, MB, GB)."""
    if not isinstance(byte_count, (int, float)) or byte_count < 0:
        return "0 B"
    if byte_count == 0:
        return "0 B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(byte_count, 1024)))
    p = math.pow(1024, i)
    s = round(byte_count / p, 1)
    return f"{s} {size_name[i]}"


def _format_duration(seconds: float) -> str:
    """Formats a duration in seconds into an HH:MM:SS string."""
    seconds = int(seconds)
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"

def _format_long_duration(seconds: float) -> str:
    """Formats a long duration in seconds into a human-readable string."""
    if not seconds or seconds < 60:
        return f"{int(seconds)} seconds"
    
    seconds = int(seconds)
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, _ = divmod(rem, 60)

    parts = []
    if days > 0:
        parts.append(f"{days} day{'s' if days > 1 else ''}")
    if hours > 0:
        parts.append(f"{hours} hour{'s' if hours > 1 else ''}")
    if minutes > 0:
        parts.append(f"{minutes} minute{'s' if minutes > 1 else ''}")
    
    return ", ".join(parts)


def generate_report(
    records: List[Dict[str, Any]],
    output_path: Path,
    *,
    is_dry_run: bool,
    source_path: str,
    dest_path: str,
    run_mode: str,
    num_workers: int,
    run_time_secs: float,
    tool_versions: Optional[Dict[str, str]] = None,
):
    """
    Generates and writes the full HTML report.
    """
    # --- 1. Process Records and Calculate Statistics ---
    stats = defaultdict(lambda: {"total": 0, "dated": 0, "undated": 0, "damaged": 0, "duplicates": 0})
    damage_stats = Counter()
    damage_locations = defaultdict(set)
    year_counts = Counter()
    
    total_files = len(records)
    damaged_count = 0
    dupe_count = 0
    dated_count = 0
    recovered_size = 0
    dupe_size = 0
    
    # Insight-specific counters
    pdf_insights = Counter()
    image_insights = Counter()
    video_insights = Counter()
    total_pdf_pages = 0 # Placeholder as it's not in the manifest

    for r in records:
        family = r.get("family", "Other")
        subtype = r.get("subtype", "UNKNOWN")
        key = (family, subtype)
        stats[key]["total"] += 1
        
        try:
            size = int(r.get("size_bytes", 0))
        except (ValueError, TypeError):
            size = 0

        is_damaged = r.get("integrity") != "ok"
        is_duplicate = bool(r.get("duplicate_of"))

        if is_damaged:
            integrity_tag = r.get("integrity", "unknown-error")
            stats[key]["damaged"] += 1
            damaged_count += 1
            damage_stats[integrity_tag] += 1
            damage_locations[integrity_tag].add(f"{family}/{subtype}")
        elif is_duplicate:
            stats[key]["duplicates"] += 1
            dupe_count += 1
            dupe_size += size
        else: # File is OK and not a duplicate
            recovered_size += size
            if r.get("undated_flag") == "1":
                stats[key]["undated"] += 1
            else:
                stats[key]["dated"] += 1
                dated_count += 1
                date_str = r.get("date_local")
                if date_str:
                    try:
                        year = datetime.fromisoformat(date_str).year
                        if 1980 < year < 2030: # Plausibility check
                            year_counts[year] += 1
                    except (ValueError, TypeError):
                        pass

        # Gather data for insight sections
        if family == 'PDFs' and not is_damaged and not is_duplicate:
            pdf_insights['total'] += 1
            pdf_kind = str(r.get('pdf_kind', '')).strip().lower()
            if pdf_kind == 'scanned':
                pdf_insights['scanned'] += 1
            elif pdf_kind == 'digital':
                pdf_insights['digital'] += 1

            if str(r.get('pdf_encrypted', '')).strip() == '1':
                pdf_insights['encrypted'] += 1
                
            if r.get('pdf_ocr_applied') == '1':
                pdfinsights['ocr_applied'] += 1
            
            # Note: total_pdf_pages may still be 0 if 'pdf_pages' is not in the manifest
            try:
                total_pdf_pages += int(r.get('pdf_pages', 0))
            except (ValueError, TypeError):
                pass # Ignore if pages is not a valid number

        elif family == 'Images' and not is_damaged and not is_duplicate:
            image_insights['total'] += 1
            if r.get('exif_make'):
                image_insights['camera_photos'] += 1
            
            if str(subtype).strip().lower() == '_screenshots':
                image_insights['screenshots'] += 1
            
            if str(r.get('iphone', '')).strip() == '1':
                image_insights['from_iphones'] += 1
        
        elif family == 'Video' and not is_damaged and not is_duplicate:
            video_insights['total'] += 1
            try:
                video_insights['total_duration_s'] += float(r.get('video_duration', 0))
            except (ValueError, TypeError):
                pass
            if r.get('video_repaired') == '1':
                video_insights['repaired'] += 1

    files_to_recover = total_files - damaged_count - dupe_count

    # --- 2. Build HTML Components ---

    report_type = "Dry Run" if is_dry_run else "Commit/Copy"
    title = f"Recovery Report ({report_type})"
    
    # --- Cards ---
    cards_html = f"""
    <div class="cards">
        <div class="card"><div class="k">Total files scanned</div><div class="v">{total_files:,}</div></div>
        <div class="card"><div class="k">Files to recover</div><div class="v">{files_to_recover:,}</div></div>
        <div class="card"><div class="k">Duplicates found</div><div class="v">{dupe_count:,}</div></div>
        <div class="card"><div class="k">Damaged files</div><div class="v">{damaged_count:,}</div></div>
        <div class="card"><div class="k">Space to recover</div><div class="v">≈ {_format_bytes(recovered_size)}</div></div>
        <div class="card"><div class="k">Space saved (dupes)</div><div class="v">≈ {_format_bytes(dupe_size)}</div></div>
    </div>
    """

    # --- What This Tool Did ---
    what_this_tool_did_html = f"""
    <h2>What This Tool Did</h2>
    <ul class="summary-list">
        <li>Scanned <b>{total_files:,} files</b> from the recovery source.</li>
        <li>Validated file structures for corruption (JPEGs, Office, PDFs, etc.).</li>
        <li>Recovered original creation date for <b>{dated_count:,} files</b> from metadata.</li>
        <li>Added a text layer to <b>{pdf_insights['scanned']:,} scanned PDFs</b> to make them searchable.</li>
        <li>Grouped all valid files into a structured hierarchy by type and date.</li>
        <li>Flagged and set aside <b>{dupe_count:,} duplicate files</b> to save space.</li>
    </ul>
    """
    
    # --- Insights Sections ---
    insights_html = f"""
    <div class="insights-grid">
        <div class="insight-card">
            <h3>Image Insights</h3>
            <dl>
                <dt>Total Images</dt><dd>{image_insights['total']:,}</dd>
                <dt>Photos from Cameras</dt><dd>{image_insights['camera_photos']:,}</dd>
                <dt>Screenshots</dt><dd>{image_insights['screenshots']:,}</dd>
                <dt>From iPhones</dt><dd>{image_insights['from_iphones']:,}</dd>
            </dl>
        </div>
        <div class="insight-card">
            <h3>PDF Insights</h3>
            <dl>
                <dt>Total PDFs</dt><dd>{pdf_insights['total']:,}</dd>
                <dt>Scanned (w/ OCR)</dt><dd>{pdf_insights['scanned']:,}</dd>
                <dt>Digital-born</dt><dd>{pdf_insights['digital']:,}</dd>
                <dt>Encrypted</dt><dd>{pdf_insights['encrypted']:,}</dd>
            </dl>
        </div>
        <div class="insight-card">
            <h3>Video Insights</h3>
            <dl>
                <dt>Total Videos</dt><dd>{video_insights['total']:,}</dd>
                <dt>Total Duration</dt><dd>{_format_long_duration(video_insights['total_duration_s'])}</dd>
                <dt>Repaired Files</dt><dd>{video_insights['repaired']:,}</dd>
            </dl>
        </div>
    </div>
    """

    # --- Date Coverage / Timeline ---
    timeline_html = ""
    if year_counts:
        min_year, max_year = min(year_counts.keys()), max(year_counts.keys())
        timeline_rows = "".join(f"<tr><td>{year}</td><td>{year_counts[year]:,} files</td></tr>" for year in sorted(year_counts.keys()))
        timeline_html = f"""
        <h2>Date Coverage <span class="pill">{min_year} – {max_year}</span></h2>
        <div class="timeline-container">
            <table>{timeline_rows}</table>
        </div>
        """

    # --- Summary Table ---
    summary_rows = []
    for (fam, sub), s in sorted(stats.items()):
        summary_rows.append(f"""
        <tr>
            <td>{fam}</td>
            <td>{sub}</td>
            <td>{s['total']:,}</td>
            <td>{s['dated']:,}</td>
            <td>{s['undated']:,}</td>
            <td>{s['damaged']:,}</td>
            <td>{s['duplicates']:,}</td>
        </tr>""")
    summary_table_html = f"<h2>File Type Breakdown</h2><table><tr><th>Family</th><th>Subtype</th><th>Total</th><th>Dated</th><th>Undated</th><th>Damaged</th><th>Duplicates</th></tr>" \
                       + "".join(summary_rows) + "</table>"

    # --- Damage Breakdown Table ---
    damage_rows = []
    for tag, count in damage_stats.most_common():
        location = sorted(list(damage_locations[tag]))[0] if damage_locations[tag] else "N/A"
        damage_rows.append(f"""
        <tr>
            <td><code>{tag}</code></td>
            <td>{count:,}</td>
            <td class="nobreak">{location} → damaged</td>
        </tr>""")
    damage_table_html = "<h2>Damaged File Details</h2><table><tr><th>Integrity tag</th><th>Count</th><th>Where</th></tr>" \
                      + "".join(damage_rows) + "</table>" if damage_rows else ""

    # --- Footer ---
    tools_str = ", ".join(f"{k} v{v}" for k,v in tool_versions.items()) if tool_versions else "N/A"
    footer_html = f"""
    <hr>
    <footer>
      Script: beachcomb.py v{__version__} &nbsp;&bull;&nbsp; Mode: {run_mode} &nbsp;&bull;&nbsp; Workers: {num_workers} &nbsp;&bull;&nbsp; Run time: {_format_duration(run_time_secs)}<br>
      Tools: {tools_str}
    </footer>
    """

    # --- 3. Assemble Final HTML Document ---
    html_doc = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<style>
  :root {{ --bg:#fff; --fg:#111; --muted:#666; --line:#e6e6e6; --accent:#007aff; }}
  body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin: 0 auto; max-width: 900px; padding: 24px; color: var(--fg); background: var(--bg); }}
  h1 {{ margin: 0 0 6px 0; font-size: 28px; }}
  h2 {{ margin: 40px 0 12px; font-size: 20px; border-bottom: 1px solid var(--line); padding-bottom: 6px;}}
  h3 {{ margin: 0 0 10px 0; font-size: 16px; }}
  .sub {{ color: var(--muted); margin-bottom: 18px; font-size: 14px; }}
  .cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px,1fr)); gap: 12px; margin: 24px 0; }}
  .card {{ border: 1px solid var(--line); border-radius: 10px; padding: 12px 14px; background: var(--bg); }}
  .card .k {{ color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: .04em; }}
  .card .v {{ font-size: 24px; margin-top: 8px; font-weight: 500; }}
  .summary-list {{ padding-left: 20px; }}
  .summary-list li {{ margin-bottom: 8px; }}
  .insights-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 16px; }}
  .insight-card {{ border: 1px solid var(--line); border-radius: 10px; padding: 16px; }}
  .insight-card dl {{ margin: 0; }}
  .insight-card dt {{ float: left; clear: left; color: var(--muted); }}
  .insight-card dd {{ text-align: right; margin-left: 0; font-weight: 500; margin-bottom: 8px; }}
  .timeline-container {{ max-height: 250px; overflow-y: auto; border: 1px solid var(--line); border-radius: 6px; padding: 4px; }}
  table {{ border-collapse: collapse; width: 100%; font-size: 14px; }}
  td, th {{ border-bottom: 1px solid var(--line); padding: 8px 12px; text-align: left; vertical-align: top; }}
  th {{ background: #f9f9f9; font-weight: 600; border-top: 1px solid var(--line); }}
  table:not(.timeline-container table) td, table:not(.timeline-container table) th {{ border-left: 1px solid var(--line); border-right: 1px solid var(--line); }}
  .timeline-container table td, .timeline-container table th {{ border: none; }}
  td:not(:first-child), th:not(:first-child) {{ text-align: right; font-variant-numeric: tabular-nums; }}
  th:first-child, td:first-child {{ text-align: left; }}
  code {{ font-family: Menlo, Monaco, Consolas, "Courier New", monospace; background: #eee; padding: 2px 4px; border-radius: 3px; font-size: 13px; }}
  .pill {{ display:inline-block; font-size:12px; padding:2px 8px; border-radius:999px; background:var(--accent); color:#fff; font-weight:500; margin-left:8px; vertical-align:middle; }}
  hr {{ border: none; border-top: 1px solid var(--line); margin: 24px 0; }}
  footer {{ font-size: 12px; color: var(--muted); }}
  .nobreak {{ white-space: nowrap; }}
  @media (max-width: 600px) {{ body {{ padding: 16px; }} h1 {{ font-size: 24px; }} h2 {{ font-size: 18px; }} .card .v {{ font-size: 20px; }} }}
</style>
</head>
<body>
    <h1>{title}</h1>
    Provisional/incomplete, for informational purposes only<br>
	the beachcomb recovery script is experimental and has bugs, do not rely on this as your source of truth<br><br>
    <div class="sub">
        Source: <code>{source_path}</code><br>
        Destination: <code>{dest_path}</code>
    </div>
    {cards_html}
    {what_this_tool_did_html}
    {insights_html}
    {timeline_html}
    {summary_table_html}
    {damage_table_html}
    {footer_html}
</body>
</html>"""

    # --- 4. Write to File ---
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html_doc)
        print(f"Report successfully generated at: {output_path}")
    except IOError as e:
        print(f"Error: Could not write report to {output_path}. Reason: {e}")

if __name__ == '__main__':
    import csv
    import time

    manifest_file = Path("manifest.csv")
    if not manifest_file.exists():
        print(f"Error: Could not find '{manifest_file}'.")
        print("Please place a manifest.csv file in the same directory to run this example.")
    else:
        with open(manifest_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            file_records = list(reader)

        generate_report(
            records=file_records,
            output_path=Path("_report/index.html"),
            is_dry_run=False,
            source_path="/Volumes/Damaged-Disk/photorec_output",
            dest_path="/Volumes/Recovery/Sorted-Files",
            run_mode="heavy",
            num_workers=8,
            run_time_secs=time.time() - time.time() + 2531, # Simulate 42m 11s
            tool_versions={"exiftool": "12.40", "qpdf": "10.3.2", "ocrmypdf": "12.7.0"},
        )
