"""
compact_nifty_parquet.py
--------------------------
Compacts the over-partitioned optimized/ Hive dataset (symbol/year/month/
expiry/strike_bucket -> ~155 tiny files/month, per export_optimized_parquet.py)
into a month-level compacted/ dataset: symbol/year/month as the only folder
partitions, with `expiry` and `strike_bucket` kept as physical Parquet
columns. Source files under optimized/ are read with hive_partitioning=True
so those values are materialized as columns before writing, and are never
modified or deleted by this script (see prequnt-prompt1.txt).

Usage:
    python compact_nifty_parquet.py --symbol NIFTY --year 2026 --month 6 --analyze
    python compact_nifty_parquet.py --symbol NIFTY --year 2026 --month 6 --dry-run
    python compact_nifty_parquet.py --symbol NIFTY --year 2026 --month 6
    python compact_nifty_parquet.py --symbol NIFTY --year 2026            # all months found in that year
    python compact_nifty_parquet.py --symbol NIFTY --all-years            # every year found under input-root
    python compact_nifty_parquet.py --symbol NIFTY --year 2026 --month 6 --resume
    python compact_nifty_parquet.py --symbol NIFTY --year 2026 --month 6 --validate
    python compact_nifty_parquet.py --symbol NIFTY --year 2026 --month 6 --benchmark
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import statistics
import time
from pathlib import Path

import polars as pl

STRIKE_INTERVAL = 50
MIN_FILE_MB = 32
PREFERRED_FILE_MB = 128
MAX_FILE_MB = 256

# Columns actually present in option_chain_new (no open/high/low/volume —
# it's a per-minute snapshot feed, not OHLC bars). Not fabricating columns
# the prompt's generic schema lists that this source doesn't have.
NUMERIC_F32 = ["close", "iv", "delta", "gamma", "theta", "vega", "rho", "spot_price"]
ROW_IDENTITY = ["timestamp", "expiry", "strike", "option_type"]
SORT_ORDER = ["expiry", "strike_bucket", "timestamp", "strike", "option_type"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("compact_nifty_parquet")


def _month_source_dir(input_root: Path, underlying: str, year: int, month: int) -> Path:
    return input_root / f"symbol={underlying}" / f"year={year:04d}" / f"month={month:02d}"


def _month_output_dir(output_root: Path, underlying: str, year: int, month: int) -> Path:
    return output_root / f"symbol={underlying}" / f"year={year:04d}" / f"month={month:02d}"


def discover_years(input_root: Path, underlying: str) -> list[int]:
    base = input_root / f"symbol={underlying}"
    years = []
    for p in sorted(base.glob("year=*")):
        try:
            years.append(int(p.name.split("=")[1]))
        except (IndexError, ValueError):
            continue
    return years


def discover_months(input_root: Path, underlying: str, year: int) -> list[int]:
    base = input_root / f"symbol={underlying}" / f"year={year:04d}"
    months = []
    for p in sorted(base.glob("month=*")):
        try:
            months.append(int(p.name.split("=")[1]))
        except (IndexError, ValueError):
            continue
    return months


def analyze_source(month_dir: Path) -> dict:
    files = sorted(month_dir.glob("expiry=*/strike_bucket=*/part-*.parquet"))
    sizes_kb = sorted(f.stat().st_size / 1024 for f in files)
    total_bytes = sum(f.stat().st_size for f in files)

    if not sizes_kb:
        return {"month_dir": str(month_dir), "file_count": 0}

    report = {
        "month_dir": str(month_dir),
        "file_count": len(sizes_kb),
        "total_size_mb": round(total_bytes / 1_048_576, 2),
        "min_file_kb": round(sizes_kb[0], 1),
        "max_file_kb": round(sizes_kb[-1], 1),
        "median_file_kb": round(statistics.median(sizes_kb), 1),
        "avg_file_kb": round(sum(sizes_kb) / len(sizes_kb), 1),
        "files_below_8kb": sum(1 for s in sizes_kb if s < 8),
        "files_below_32kb": sum(1 for s in sizes_kb if s < 32),
        "files_below_1mb": sum(1 for s in sizes_kb if s < 1024),
        "files_below_10mb": sum(1 for s in sizes_kb if s < 10 * 1024),
        "files_above_64mb": sum(1 for s in sizes_kb if s > 64 * 1024),
    }
    return report


def load_source(month_dir: Path) -> pl.DataFrame:
    pattern = str(month_dir / "expiry=*/strike_bucket=*/part-*.parquet")
    lf = pl.scan_parquet(pattern, hive_partitioning=True, parallel="auto", rechunk=False, low_memory=False)
    return lf.collect()


def normalize(df: pl.DataFrame) -> tuple[pl.DataFrame, int]:
    # underlying/symbol/year/month are redundant with the compacted output's own
    # symbol=/year=/month= folder partitions — hive_partitioning=True on the SOURCE
    # scan materializes all of these from the source path (not just expiry/
    # strike_bucket), so they must be dropped here or they'd duplicate into every
    # row of the compacted file, same anti-pattern as the earlier exporter bug.
    df = df.drop([c for c in df.columns if c in ("underlying", "symbol", "year", "month")])

    cast_exprs = [
        pl.col("timestamp").cast(pl.Datetime("ms")),
        pl.col("expiry").cast(pl.Date),
        pl.col("strike_bucket").cast(pl.Int32),
        pl.col("strike").cast(pl.Int32),
        pl.col("atm_strike").cast(pl.Int32),
        pl.col("option_type").cast(pl.Categorical),
    ] + [pl.col(c).cast(pl.Float32) for c in NUMERIC_F32 if c in df.columns]
    if "oi" in df.columns:
        cast_exprs.append(pl.col("oi").fill_null(0).cast(pl.Int64))
    if "trade_date" in df.columns:
        cast_exprs.append(pl.col("trade_date").cast(pl.Date))

    df = df.with_columns(cast_exprs)

    before = df.height
    df = df.unique(subset=ROW_IDENTITY, keep="first")
    duplicate_rows = before - df.height

    df = df.sort(SORT_ORDER)
    return df, duplicate_rows


def _write_one(chunk: pl.DataFrame, path: Path, compression: str, row_group_size: int,
                compression_level: int | None) -> Path:
    tmp = path.with_suffix(path.suffix + ".tmp")
    write_kwargs = {"compression": compression, "statistics": True, "row_group_size": row_group_size}
    if compression_level is not None:
        write_kwargs["compression_level"] = compression_level
    chunk.write_parquet(tmp, **write_kwargs)
    tmp.rename(path)
    return path


def chunk_and_write(df: pl.DataFrame, out_dir: Path, row_group_size: int, compression: str,
                     target_file_size_mb: int, compression_level: int | None = None) -> dict:
    building_dir = out_dir.with_name(out_dir.name + ".__building__")
    if building_dir.exists():
        shutil.rmtree(building_dir)
    building_dir.mkdir(parents=True)

    row_count = df.height
    target_bytes = target_file_size_mb * 1_048_576
    max_bytes = MAX_FILE_MB * 1_048_576

    # df.estimated_size() is uncompressed in-memory size — zstd/snappy shrink actual
    # output well below that, so estimating file count from it over-splits a month
    # that's naturally small once compressed (e.g. 73MB in-memory-estimate-implied-2
    # files, actually fits in one). Write once, measure the REAL compressed size,
    # and only split if that measured size exceeds the max — no guessing.
    probe_path = _write_one(df, building_dir / "part-000.parquet", compression, row_group_size, compression_level)
    actual_bytes = probe_path.stat().st_size

    if actual_bytes <= max_bytes:
        return {
            "building_dir": building_dir,
            "files_written": 1,
            "paths": [probe_path],
            "actual_output_bytes": actual_bytes,
        }

    # Single file exceeded the max — re-split using the *measured* compression ratio.
    probe_path.unlink()
    bytes_per_row = actual_bytes / row_count
    n_files = max(2, -(-actual_bytes // target_bytes))  # ceil div
    while n_files > 1 and (actual_bytes / n_files) < MIN_FILE_MB * 1_048_576:
        n_files -= 1

    rows_per_file = -(-row_count // n_files)
    written = []
    start = 0
    idx = 0
    while start < row_count:
        chunk = df.slice(start, rows_per_file)
        path = _write_one(chunk, building_dir / f"part-{idx:03d}.parquet", compression, row_group_size, compression_level)
        written.append(path)
        start += rows_per_file
        idx += 1

    return {
        "building_dir": building_dir,
        "files_written": len(written),
        "paths": written,
        "estimated_bytes_per_row": bytes_per_row,
    }


def validate(source_month_dir: Path, output_dir: Path) -> dict:
    src = pl.scan_parquet(
        str(source_month_dir / "expiry=*/strike_bucket=*/part-*.parquet"), hive_partitioning=True
    )
    out = pl.scan_parquet(str(output_dir / "part-*.parquet"))

    src_rows = src.select(pl.len()).collect().item()
    out_rows = out.select(pl.len()).collect().item()

    def _stats(lf: pl.LazyFrame) -> dict:
        return lf.select(
            pl.col("expiry").n_unique().alias("expiries"),
            pl.col("strike").n_unique().alias("strikes"),
            pl.col("strike_bucket").n_unique().alias("strike_buckets"),
            pl.col("timestamp").min().alias("ts_min"),
            pl.col("timestamp").max().alias("ts_max"),
            pl.col("strike").min().alias("strike_min"),
            pl.col("strike").max().alias("strike_max"),
            pl.col("close").cast(pl.Float64).sum().alias("close_sum"),
            pl.col("close").null_count().alias("null_close"),
            pl.col("delta").null_count().alias("null_delta"),
            pl.col("spot_price").null_count().alias("null_spot_price"),
        ).collect().row(0, named=True)

    src_stats = _stats(src)
    out_stats = _stats(out)

    src_ce = src.filter(pl.col("option_type") == "CE").select(pl.len()).collect().item()
    out_ce = out.filter(pl.col("option_type") == "CE").select(pl.len()).collect().item()
    src_pe = src.filter(pl.col("option_type") == "PE").select(pl.len()).collect().item()
    out_pe = out.filter(pl.col("option_type") == "PE").select(pl.len()).collect().item()

    result = {
        "source_rows": src_rows,
        "output_rows": out_rows,
        "row_count_match": src_rows == out_rows,
        "expiry_count_match": src_stats["expiries"] == out_stats["expiries"],
        "strike_count_match": src_stats["strikes"] == out_stats["strikes"],
        "strike_bucket_count_match": src_stats["strike_buckets"] == out_stats["strike_buckets"],
        "timestamp_range_match": (src_stats["ts_min"], src_stats["ts_max"]) == (out_stats["ts_min"], out_stats["ts_max"]),
        "strike_range_match": (src_stats["strike_min"], src_stats["strike_max"]) == (out_stats["strike_min"], out_stats["strike_max"]),
        "ce_row_count_match": src_ce == out_ce,
        "pe_row_count_match": src_pe == out_pe,
        "close_checksum_match": abs(src_stats["close_sum"] - out_stats["close_sum"]) < 1e-6,
        "null_counts_match": (
            src_stats["null_close"] == out_stats["null_close"]
            and src_stats["null_delta"] == out_stats["null_delta"]
            and src_stats["null_spot_price"] == out_stats["null_spot_price"]
        ),
        "source_expiries": src_stats["expiries"],
        "output_expiries": out_stats["expiries"],
        "source_strike_buckets": src_stats["strike_buckets"],
        "output_strike_buckets": out_stats["strike_buckets"],
    }
    result["status"] = "passed" if all(
        result[k] for k in (
            "row_count_match", "expiry_count_match", "strike_count_match", "strike_bucket_count_match",
            "timestamp_range_match", "strike_range_match",
            "ce_row_count_match", "pe_row_count_match", "close_checksum_match", "null_counts_match",
        )
    ) else "failed"
    return result


BENCHMARK_COLUMNS = ["timestamp", "expiry", "strike", "option_type", "close", "delta", "spot_price", "atm_strike"]


def run_benchmark(source_dir: Path, output_dir: Path, n_runs: int = 5) -> dict:
    """Benchmarks 1-4 from prequnt-prompt1.txt: full-month scan, one-expiry filter,
    expiry+strike_bucket filter, exact ATM+/-20 row filter — source layout vs
    compacted layout, same filters, warm cache, median/min/max of n_runs."""
    src_pattern = str(source_dir / "expiry=*/strike_bucket=*/part-*.parquet")
    out_files = [str(p) for p in sorted(output_dir.glob("part-*.parquet"))]
    if not out_files:
        raise FileNotFoundError(f"no compacted part files under {output_dir}")

    rep_expiry = (
        pl.scan_parquet(src_pattern, hive_partitioning=True)
        .group_by("expiry").agg(pl.len().alias("n")).sort("n", descending=True)
        .limit(1).collect()["expiry"][0]
    )
    strike_buckets = (
        pl.scan_parquet(src_pattern, hive_partitioning=True)
        .filter(pl.col("expiry") == rep_expiry)
        .select(pl.col("strike_bucket").unique()).collect()["strike_bucket"].sort().to_list()
    )

    def src_scan() -> pl.LazyFrame:
        return pl.scan_parquet(src_pattern, hive_partitioning=True, parallel="auto", rechunk=False, low_memory=False)

    def out_scan() -> pl.LazyFrame:
        return pl.scan_parquet(out_files, parallel="auto", rechunk=False, low_memory=False)

    def timeit(fn) -> dict:
        times, rows = [], None
        for _ in range(n_runs):
            t0 = time.perf_counter()
            r = fn()
            times.append((time.perf_counter() - t0) * 1000)
            rows = r.height
        times.sort()
        mid = len(times) // 2
        return {"rows": rows, "median_ms": round(times[mid], 1), "min_ms": round(times[0], 1), "max_ms": round(times[-1], 1)}

    atm_filter = (pl.col("strike") >= pl.col("atm_strike") - 1000) & (pl.col("strike") <= pl.col("atm_strike") + 1000)

    return {
        "representative_expiry": str(rep_expiry),
        "strike_buckets_for_expiry": strike_buckets,
        "n_runs": n_runs,
        "benchmark_1_full_month_scan": {
            "source": timeit(lambda: src_scan().select(BENCHMARK_COLUMNS).collect()),
            "compacted": timeit(lambda: out_scan().select(BENCHMARK_COLUMNS).collect()),
        },
        "benchmark_2_expiry_filter": {
            "source": timeit(lambda: src_scan().filter(pl.col("expiry") == rep_expiry).select(BENCHMARK_COLUMNS).collect()),
            "compacted": timeit(lambda: out_scan().filter(pl.col("expiry") == rep_expiry).select(BENCHMARK_COLUMNS).collect()),
        },
        "benchmark_3_expiry_and_strike_buckets": {
            "source": timeit(lambda: src_scan().filter(
                (pl.col("expiry") == rep_expiry) & (pl.col("strike_bucket").is_in(strike_buckets))
            ).select(BENCHMARK_COLUMNS).collect()),
            "compacted": timeit(lambda: out_scan().filter(
                (pl.col("expiry") == rep_expiry) & (pl.col("strike_bucket").is_in(strike_buckets))
            ).select(BENCHMARK_COLUMNS).collect()),
        },
        "benchmark_4_atm_pm20_filter": {
            "source": timeit(lambda: src_scan().filter(atm_filter).select(BENCHMARK_COLUMNS).collect()),
            "compacted": timeit(lambda: out_scan().filter(atm_filter).select(BENCHMARK_COLUMNS).collect()),
        },
    }


def compact_month(input_root: Path, output_root: Path, underlying: str, year: int, month: int,
                   row_group_size: int = 100_000, compression: str = "zstd", compression_level: int | None = 1,
                   target_file_size_mb: int = 128, force: bool = False, dry_run: bool = False) -> dict:
    source_dir = _month_source_dir(input_root, underlying, year, month)
    output_dir = _month_output_dir(output_root, underlying, year, month)

    if not source_dir.exists():
        raise FileNotFoundError(f"no source data at {source_dir}")

    analysis = analyze_source(source_dir)
    log.info("source analysis: %s", json.dumps(analysis))

    if output_dir.exists() and not force:
        raise FileExistsError(f"{output_dir} already exists; pass force=True to overwrite")

    if dry_run:
        log.info("[dry-run] would compact %s -> %s (no files written)", source_dir, output_dir)
        return {"dry_run": True, "source_analysis": analysis}

    t0 = time.perf_counter()
    raw = load_source(source_dir)
    load_seconds = time.perf_counter() - t0

    t0 = time.perf_counter()
    df, duplicate_rows = normalize(raw)
    normalize_seconds = time.perf_counter() - t0

    t0 = time.perf_counter()
    write_result = chunk_and_write(df, output_dir, row_group_size, compression, target_file_size_mb, compression_level)
    write_seconds = time.perf_counter() - t0

    validation = validate(source_dir, write_result["building_dir"])
    if validation["status"] != "passed":
        shutil.rmtree(write_result["building_dir"])
        raise RuntimeError(f"validation failed, compacted output discarded: {validation}")

    if output_dir.exists():
        shutil.rmtree(output_dir)
    write_result["building_dir"].rename(output_dir)

    output_bytes = sum(p.stat().st_size for p in output_dir.glob("part-*.parquet"))

    report = {
        "symbol": underlying,
        "year": year,
        "month": month,
        "source_analysis": analysis,
        "duplicate_rows_removed": duplicate_rows,
        "output_dir": str(output_dir),
        "output_files": write_result["files_written"],
        "output_total_mb": round(output_bytes / 1_048_576, 2),
        "load_seconds": round(load_seconds, 2),
        "normalize_seconds": round(normalize_seconds, 2),
        "write_seconds": round(write_seconds, 2),
        "validation": validation,
    }
    log.info("compaction done: %s", json.dumps(report, default=str))
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Compact over-partitioned optimized/ NIFTY Parquet into month-level files")
    parser.add_argument("--symbol", default="NIFTY")
    parser.add_argument("--year", type=int, help="required unless --all-years is given")
    parser.add_argument("--month", type=int, help="1-12; omit to process every month found for --year")
    parser.add_argument("--all-years", action="store_true", help="process every year found under --input-root for this symbol")
    parser.add_argument("--input-root", default=str(Path(__file__).resolve().parent / "parquet_data" / "optimized"))
    parser.add_argument("--output-root", default=str(Path(__file__).resolve().parent / "parquet_data" / "compacted"))
    parser.add_argument("--row-group-size", type=int, default=100_000, help="benchmarked: 100k beats 250k/500k on filtered-read speed for this dataset")
    parser.add_argument("--compression", default="zstd", help="benchmarked: zstd level 1 beats snappy on both size and read speed for this dataset")
    parser.add_argument("--compression-level", type=int, default=1)
    parser.add_argument("--target-file-size-mb", type=int, default=128)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--resume", action="store_true", help="skip a month if its compacted output already exists and validates against source")
    parser.add_argument("--analyze", action="store_true", help="only print the small-file analysis report, no compaction")
    parser.add_argument("--validate", action="store_true", help="only validate existing compacted output against source, no compaction")
    parser.add_argument("--benchmark", action="store_true", help="only run source-vs-compacted query benchmarks, no compaction")
    args = parser.parse_args()

    if not args.all_years and args.year is None:
        parser.error("--year is required unless --all-years is given")

    input_root, output_root = Path(args.input_root), Path(args.output_root)

    targets: list[tuple[int, int]] = []
    if args.all_years:
        for y in discover_years(input_root, args.symbol):
            targets.extend((y, m) for m in discover_months(input_root, args.symbol, y))
    elif args.month is not None:
        targets.append((args.year, args.month))
    else:
        targets.extend((args.year, m) for m in discover_months(input_root, args.symbol, args.year))

    if not targets:
        log.warning("no source months found for symbol=%s under %s", args.symbol, input_root)
        return

    processed = 0
    for year, month in targets:
        source_dir = _month_source_dir(input_root, args.symbol, year, month)
        output_dir = _month_output_dir(output_root, args.symbol, year, month)

        if args.analyze:
            print(json.dumps({"year": year, "month": month, **analyze_source(source_dir)}, indent=2))
            continue

        if args.validate:
            if not output_dir.exists():
                print(json.dumps({"year": year, "month": month, "status": "no_compacted_output"}, indent=2))
                continue
            print(json.dumps({"year": year, "month": month, **validate(source_dir, output_dir)}, indent=2, default=str))
            continue

        if args.benchmark:
            if not output_dir.exists():
                print(json.dumps({"year": year, "month": month, "status": "no_compacted_output"}, indent=2))
                continue
            print(json.dumps({"year": year, "month": month, **run_benchmark(source_dir, output_dir)}, indent=2, default=str))
            continue

        force = args.force
        if args.resume and output_dir.exists():
            if validate(source_dir, output_dir)["status"] == "passed":
                log.info("resume: %04d-%02d already compacted and valid, skipping", year, month)
                continue
            force = True  # existing output present but stale/invalid -> overwrite

        report = compact_month(
            input_root, output_root, args.symbol, year, month,
            row_group_size=args.row_group_size, compression=args.compression, compression_level=args.compression_level,
            target_file_size_mb=args.target_file_size_mb, force=force, dry_run=args.dry_run,
        )
        print(json.dumps(report, indent=2, default=str))
        processed += 1

    if len(targets) > 1 and not (args.analyze or args.validate or args.benchmark):
        print(json.dumps({"months_requested": len(targets), "months_compacted": processed}, indent=2))


if __name__ == "__main__":
    main()
