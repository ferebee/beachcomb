#!/usr/bin/env python3
import argparse, hashlib, os, sys, csv, time, json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

CHUNK = 2 * 1024 * 1024  # 2 MiB

def hash_file(p: Path, algo: str) -> str:
    if algo == "sha256":
        h = hashlib.sha256()
    elif algo == "sha1":
        h = hashlib.sha1()
    elif algo == "md5":
        h = hashlib.md5()
    else:
        raise ValueError(f"Unsupported algo: {algo}")
    with p.open("rb") as f:
        while True:
            b = f.read(CHUNK)
            if not b: break
            h.update(b)
    return h.hexdigest()

def walk_files(root: Path):
    for dirpath, dirnames, filenames in os.walk(root):
        # skip common noise; add more if you like
        dirnames[:] = [d for d in dirnames if d not in {".git", ".Trash"}]
        for name in filenames:
            yield Path(dirpath) / name

def build_manifest(root: Path, algo: str, workers: int, out_csv: Path):
    files = list(walk_files(root))
    rows = []
    total_bytes = 0
    t0 = time.time()

    def work(p: Path):
        try:
            if p.is_symlink() or not p.is_file():
                return None
            rel = p.relative_to(root)
            size = p.stat().st_size
            h = hash_file(p, algo)
            return (h, size, str(rel))
        except Exception as e:
            return (f"ERROR:{e.__class__.__name__}", 0, str(p))

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(work, p): p for p in files}
        for i, fut in enumerate(as_completed(futs), 1):
            r = fut.result()
            if r is None:
                continue
            rows.append(r)
            if isinstance(r[1], int):
                total_bytes += r[1]
            if i % 1000 == 0:
                elapsed = time.time() - t0
                print(f"[{out_csv.name}] {i}/{len(files)} hashed in {elapsed:0.1f}s", file=sys.stderr)

    rows.sort(key=lambda t: (t[0], t[2]))  # by hash, then relpath
    with out_csv.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["hash", "size", "relpath"])
        w.writerows(rows)

    totals = {
        "files": len([r for r in rows if not str(r[0]).startswith("ERROR:")]),
        "bytes": total_bytes,
        "unique_hashes": len({r[0] for r in rows if not str(r[0]).startswith("ERROR:")}),
        "errors": len([r for r in rows if str(r[0]).startswith("ERROR:")]),
    }
    return totals

def load_index(csv_path: Path):
    """
    Returns:
      hashes: set of hashes (excluding ERROR rows)
      idx: dict hash -> {"size": int, "paths": [relpaths]}
    """
    idx = {}
    hashes = set()
    with csv_path.open() as f:
        r = csv.DictReader(f)
        for row in r:
            h = row["hash"]
            if h.startswith("ERROR:"):
                continue
            size = int(row["size"]) if row["size"].isdigit() else 0
            rel = row["relpath"]
            if h not in idx:
                idx[h] = {"size": size, "paths": [rel]}
            else:
                # sanity: same size for identical content; keep first
                idx[h]["paths"].append(rel)
            hashes.add(h)
    return hashes, idx

def write_hash_only(csv_path: Path, idx_side: dict, hashes: set, other_hashes: set, label_for_paths: str):
    """
    Write per-hash rows for hashes present only in idx_side (not in other_hashes).
    Columns: hash, size, count, total_bytes, <label_for_paths>
    Paths are '|' separated to keep CSV simple.
    Returns count of hashes written.
    """
    only = sorted(hashes - other_hashes)
    with csv_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["hash", "size", "count", "total_bytes", label_for_paths])
        for h in only:
            info = idx_side[h]
            paths = info["paths"]
            size = info["size"]
            count = len(paths)
            total_bytes = size * count
            w.writerow([h, size, count, total_bytes, " | ".join(paths)])
    return len(only)

def main():
    ap = argparse.ArgumentParser(description="Hash-only audit of source vs destination with per-hash path listings.")
    ap.add_argument("source")
    ap.add_argument("dest")
    ap.add_argument("--algo", default="sha256", help="sha256|sha1|md5 (default: sha256)")
    ap.add_argument("--workers", type=int, default=os.cpu_count() or 8)
    ap.add_argument("--outdir", default=".")
    args = ap.parse_args()

    src = Path(args.source).expanduser().resolve()
    dst = Path(args.dest).expanduser().resolve()
    outdir = Path(args.outdir).expanduser().resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    src_csv = outdir / "source_manifest.csv"
    dst_csv = outdir / "dest_manifest.csv"

    print("Building source manifest...", file=sys.stderr)
    src_tot = build_manifest(src, args.algo, args.workers, src_csv)
    print("Building dest manifest...", file=sys.stderr)
    dst_tot = build_manifest(dst, args.algo, args.workers, dst_csv)

    # Build hash indexes
    src_hashes, src_idx = load_index(src_csv)
    dst_hashes, dst_idx = load_index(dst_csv)

    # Per-hash “content only in …” lists (with all paths)
    missing_csv = outdir / "content_only_in_source.csv"  # present in src, absent in dest
    extra_csv   = outdir / "content_only_in_dest.csv"    # present in dest, absent in src
    missing_count = write_hash_only(missing_csv, src_idx, src_hashes, dst_hashes, "source_paths")
    extra_count   = write_hash_only(extra_csv, dst_idx, dst_hashes, src_hashes, "dest_paths")

    # Legacy minimalist lists (hash + one example path), still useful for quick eyeballing
    with (outdir / "missing_in_dest.csv").open("w", newline="") as f:
        w = csv.writer(f); w.writerow(["hash", "example_source_relpath"])
        for h in sorted(src_hashes - dst_hashes):
            w.writerow([h, src_idx[h]["paths"][0]])

    with (outdir / "extra_in_dest.csv").open("w", newline="") as f:
        w = csv.writer(f); w.writerow(["hash", "example_dest_relpath"])
        for h in sorted(dst_hashes - src_hashes):
            w.writerow([h, dst_idx[h]["paths"][0]])

    # Dedup estimate
    dedup_eliminated = src_tot["files"] - len(src_hashes)

    # Summary
    with (outdir / "summary.txt").open("w") as f:
        def line(s=""): print(s, file=f)
        line("==== beachcomb Hash Audit Summary ====")
        line(f"Algorithm: {args.algo}")
        line(f"Source:      {src}")
        line(f"Destination: {dst}")
        line()
        line(f"Source files:           {src_tot['files']:,}")
        line(f"Source bytes:           {src_tot['bytes']:,}")
        line(f"Source unique hashes:   {len(src_hashes):,}")
        line(f"Source hashing errors:  {src_tot['errors']:,}")
        line()
        line(f"Dest files:             {dst_tot['files']:,}")
        line(f"Dest bytes:             {dst_tot['bytes']:,}")
        line(f"Dest unique hashes:     {len(dst_hashes):,}")
        line(f"Dest hashing errors:    {dst_tot['errors']:,}")
        line()
        line(f"Likely duplicates eliminated from source: {dedup_eliminated:,}")
        line(f"Content only in source (hashes):          {missing_count:,}")
        line(f"Content only in dest (hashes):            {extra_count:,}")
        line()
        line("Detailed per-hash listings:")
        line(f"- content_only_in_source.csv")
        line(f"- content_only_in_dest.csv")
        line("Quick lists:")
        line(f"- missing_in_dest.csv (hash + one source path)")
        line(f"- extra_in_dest.csv   (hash + one dest path)")
        line("Manifests: source_manifest.csv, dest_manifest.csv")

    print("Done. Wrote:")
    for p in [src_csv, dst_csv,
              outdir / "content_only_in_source.csv",
              outdir / "content_only_in_dest.csv",
              outdir / "missing_in_dest.csv",
              outdir / "extra_in_dest.csv",
              outdir / "summary.txt"]:
        print(f"- {p}")

if __name__ == "__main__":
    main()
