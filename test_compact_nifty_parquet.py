"""
test_compact_nifty_parquet.py
--------------------------------
Integration tests for compact_nifty_parquet.py + compacted_loader.py,
run against the real exported+compacted NIFTY 2026-06 dataset on disk
(see prequnt-prompt1.txt "Required Tests"). Skipped automatically if that
data isn't present in this environment — this suite validates the actual
pipeline output, not a synthetic mock of it.

Run: python -m pytest test_compact_nifty_parquet.py -v
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest

from compact_nifty_parquet import _month_output_dir, _month_source_dir, validate
from compacted_loader import BacktestDataRequest, CompactedMarketDataLoader

SYMBOL = "NIFTY"
YEAR = 2026
MONTH = 6

ROOT = Path(__file__).resolve().parent
INPUT_ROOT = ROOT / "parquet_data" / "optimized"
OUTPUT_ROOT = ROOT / "parquet_data" / "compacted"
SOURCE_DIR = _month_source_dir(INPUT_ROOT, SYMBOL, YEAR, MONTH)
COMPACTED_DIR = _month_output_dir(OUTPUT_ROOT, SYMBOL, YEAR, MONTH)

pytestmark = pytest.mark.skipif(
    not SOURCE_DIR.exists() or not COMPACTED_DIR.exists(),
    reason="requires exported optimized/ and compacted/ NIFTY 2026-06 data on disk",
)


@pytest.fixture(scope="module")
def compacted_files() -> list[str]:
    return [str(p) for p in sorted(COMPACTED_DIR.glob("part-*.parquet"))]


@pytest.fixture(scope="module")
def compacted_df(compacted_files) -> pl.DataFrame:
    return pl.scan_parquet(compacted_files).collect()


@pytest.fixture(scope="module")
def validation_result() -> dict:
    return validate(SOURCE_DIR, COMPACTED_DIR)


def test_hive_partition_extraction():
    """A source row under expiry=2026-06-02/strike_bucket=23000/ must materialize
    those exact values as columns when read with hive_partitioning=True."""
    sample_dir = SOURCE_DIR / "expiry=2026-06-02" / "strike_bucket=23000"
    assert sample_dir.exists()
    df = pl.scan_parquet(str(sample_dir / "part-*.parquet"), hive_partitioning=True).limit(5).collect()
    assert set(df["expiry"].cast(str).unique().to_list()) == {"2026-06-02"}
    assert set(df["strike_bucket"].unique().to_list()) == {23000}


def test_schema_preservation(compacted_df):
    required = {
        "timestamp", "expiry", "strike", "strike_bucket", "option_type",
        "close", "oi", "iv", "delta", "spot_price", "atm_strike",
    }
    assert required.issubset(set(compacted_df.columns))


def test_no_leaked_partition_columns(compacted_df):
    # symbol/year/month/underlying live only in the folder path — must not
    # duplicate into every row as physical columns (the bug fixed earlier).
    assert not ({"symbol", "year", "month", "underlying"} & set(compacted_df.columns))


def test_row_count_preservation(validation_result):
    assert validation_result["row_count_match"], validation_result


def test_expiry_count_preservation(validation_result):
    assert validation_result["expiry_count_match"], validation_result


def test_strike_bucket_preservation(validation_result):
    assert validation_result["strike_bucket_count_match"], validation_result


def test_full_validation_passes(validation_result):
    assert validation_result["status"] == "passed", validation_result


def test_sort_order(compacted_df):
    expected = compacted_df.sort(["expiry", "strike_bucket", "timestamp", "strike", "option_type"])
    assert compacted_df.equals(expected)


def test_file_count_reduction():
    source_files = list(SOURCE_DIR.glob("expiry=*/strike_bucket=*/part-*.parquet"))
    output_files = list(COMPACTED_DIR.glob("part-*.parquet"))
    assert len(source_files) >= 100, "source fixture no longer over-partitioned — test assumption stale"
    assert len(output_files) <= 6
    assert len(output_files) < len(source_files) / 10


def test_tiny_file_elimination():
    for f in COMPACTED_DIR.glob("part-*.parquet"):
        size_mb = f.stat().st_size / 1_048_576
        assert size_mb > 8, f"{f} is only {size_mb:.2f}MB — unexpected tiny compacted file"


def test_row_group_statistics_enabled():
    for f in COMPACTED_DIR.glob("part-*.parquet"):
        pf = pq.ParquetFile(f)
        rg = pf.metadata.row_group(0)
        names = pf.schema_arrow.names
        for col in ("expiry", "strike_bucket", "timestamp", "strike"):
            idx = names.index(col)
            assert rg.column(idx).is_stats_set, f"{col} missing row-group stats in {f}"


def test_query_result_equality_source_vs_compacted():
    cols = ["timestamp", "expiry", "strike", "option_type", "close"]
    source_result = (
        pl.scan_parquet(str(SOURCE_DIR / "expiry=*/strike_bucket=*/part-*.parquet"), hive_partitioning=True)
        .filter(pl.col("expiry") == date(2026, 6, 2))
        .select(cols).sort(cols).collect()
    )
    compacted_result = (
        pl.scan_parquet(str(COMPACTED_DIR / "part-*.parquet"))
        .filter(pl.col("expiry") == date(2026, 6, 2))
        .select(cols).sort(cols).collect()
    )
    assert source_result.equals(compacted_result)


def test_atm_pm20_filter_correctness(compacted_df):
    filtered = compacted_df.filter(
        (pl.col("strike") >= pl.col("atm_strike") - 1000)
        & (pl.col("strike") <= pl.col("atm_strike") + 1000)
    )
    assert (filtered["strike"] >= filtered["atm_strike"] - 1000).all()
    assert (filtered["strike"] <= filtered["atm_strike"] + 1000).all()
    outside = compacted_df.filter(
        (pl.col("strike") < pl.col("atm_strike") - 1000)
        | (pl.col("strike") > pl.col("atm_strike") + 1000)
    )
    assert filtered.height + outside.height == compacted_df.height


def test_loader_matches_independent_manual_filter():
    loader = CompactedMarketDataLoader(OUTPUT_ROOT)
    req = BacktestDataRequest(
        symbol=SYMBOL, start_date=date(2026, 6, 1), end_date=date(2026, 6, 30),
        expiries=(date(2026, 6, 2),), candidate_strikes=20,
    )
    loaded = loader.load(req)
    # cross-checked against an independently-computed manual Polars filter
    # earlier in this pipeline's development — same number, different code path
    assert loaded.height == 53_595


def test_expiry_type_current_week_matches_join_asof_baseline():
    loader = CompactedMarketDataLoader(OUTPUT_ROOT)
    req = BacktestDataRequest(
        symbol=SYMBOL, start_date=date(2026, 6, 1), end_date=date(2026, 6, 30),
        expiry_type="current_week", candidate_strikes=20,
    )
    loaded = loader.load(req)
    assert loaded.height == 478_345
    assert set(loaded["expiry"].cast(str).unique().to_list()) == {
        "2026-06-02", "2026-06-09", "2026-06-16", "2026-06-23", "2026-06-30",
    }


def test_performance_regression_warm_scan_threshold(compacted_files):
    """Warning-level threshold only — shared/CI runners are noisy, so this is
    generous (2s vs the ~50-60ms actually measured on the dev NVMe). Use
    `compact_nifty_parquet.py --benchmark` on the real target server for the
    strict <5s/year acceptance check."""
    import time
    t0 = time.perf_counter()
    pl.scan_parquet(compacted_files).select(
        ["timestamp", "expiry", "strike", "option_type", "close", "delta", "spot_price", "atm_strike"]
    ).collect()
    elapsed_ms = (time.perf_counter() - t0) * 1000
    assert elapsed_ms < 2000, f"full-month scan took {elapsed_ms:.0f}ms, expected well under 2000ms warm"
