"""Pytest suite comparing Stage 1 output against open-source OSBAP data.

Loads both datasets, aggregates Stage 1 daily → month-end, merges on
(cusip, date), and asserts that overlapping columns are close.

Reads:
    _data/pulled/corporate_bond_returns.parquet  (from pull_open_source_bond.py)
    stage1/data/stage1_YYYYMMDD.parquet   (from Stage 1 pipeline)

Run:
    pytest src/test_stage1_vs_open_source.py -v --junitxml=_output/test_results.xml
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl
import pytest

# ---------------------------------------------------------------------------
# Resolve paths via settings.py
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent))
from settings import config

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))
BASE_DIR = DATA_DIR.parent

# ---------------------------------------------------------------------------
# Thresholds – tighten or loosen as needed
# ---------------------------------------------------------------------------
MIN_MERGE_ROWS = 100  # Minimum rows after inner join
MIN_CORRELATION = 0.90  # Minimum Pearson r for continuous columns
MAX_MEDIAN_ABS_PCT_DIFF = 0.05  # 5% median absolute percentage difference
MIN_PCT_WITHIN_1PCT = 49.0  # At least 49% of rows within 1% (allows for winsorization differences)

# Column mapping: (stage1_col, osbap_col, description, is_continuous)
# Note: OSBAP 'sze' = bond market cap (dirty_price/100 * face_value/1000) in $M.
# We derive 'sze_computed' from prfull and bond_amt_outstanding in the fixture.
COLUMN_MAP = [
    ("sze_computed", "sze", "Bond Market Cap ($M)", True),
    ("ytm", "ytm", "Yield to Maturity", True),
    ("mod_dur", "md_dur", "Modified Duration", True),
    ("convexity", "convx", "Convexity", True),
    ("credit_spread", "cs", "Credit Spread", True),
    ("bond_maturity", "tmat", "Time to Maturity", True),
    ("bond_amt_outstanding", "fce_val", "Amount Outstanding", True),
    ("bond_age", "age", "Bond Age", True),
    ("ff17num", "ff17num", "FF17 Industry", False),
    ("ff30num", "ff30num", "FF30 Industry", False),
]


# ---------------------------------------------------------------------------
# Fixtures – load and merge once, share across all tests
# ---------------------------------------------------------------------------
def _find_stage1_file():
    stage1_data_dir = BASE_DIR / "stage1" / "data"
    stage1_files = sorted(stage1_data_dir.glob("stage1_*.parquet"))
    if not stage1_files:
        raise FileNotFoundError(
            f"No stage1 output found in {stage1_data_dir}. "
            "Run the Stage 1 pipeline first."
        )
    return stage1_files[-1]


def _load_stage1_monthly(path):
    df = pl.read_parquet(path)
    df = df.with_columns(pl.col("trd_exctn_dt").cast(pl.Date))
    # Month-end: use Polars .dt.month_end()
    df = df.with_columns(
        pl.col("trd_exctn_dt").dt.month_end().alias("month_end")
    )
    df = df.sort("trd_exctn_dt")
    # Group by cusip_id + month_end, take last row
    df = df.group_by(["cusip_id", "month_end"]).last()
    return df


def _load_osbap():
    osbap_path = DATA_DIR / "pulled" / "corporate_bond_returns.parquet"
    if not osbap_path.exists():
        raise FileNotFoundError(
            f"{osbap_path} not found. Run pull_open_source_bond.py first."
        )
    df = pl.read_parquet(osbap_path)
    df = df.with_columns(pl.col("date").cast(pl.Date))
    return df


@pytest.fixture(scope="session")
def merged():
    """Inner-join stage1 (month-end) with OSBAP on (cusip, date)."""
    stage1 = _load_stage1_monthly(_find_stage1_file())
    osbap = _load_osbap()

    stage1_m = stage1.rename({"cusip_id": "cusip", "month_end": "date"})
    # Derive sze (bond market cap in $M) = dirty_price/100 * amt_outstanding/1000
    stage1_m = stage1_m.with_columns(
        (pl.col("prfull") / 100.0 * pl.col("bond_amt_outstanding") / 1000.0)
        .alias("sze_computed")
    )
    # Cast cusip to String so Polars can join across different Categorical sources
    stage1_m = stage1_m.with_columns(pl.col("cusip").cast(pl.Utf8))
    osbap = osbap.with_columns(pl.col("cusip").cast(pl.Utf8))

    # Restrict to overlapping date range
    min_date = max(stage1_m["date"].min(), osbap["date"].min())
    max_date = min(stage1_m["date"].max(), osbap["date"].max())
    stage1_m = stage1_m.filter(
        (pl.col("date") >= min_date) & (pl.col("date") <= max_date)
    )
    osbap = osbap.filter(
        (pl.col("date") >= min_date) & (pl.col("date") <= max_date)
    )

    merged = stage1_m.join(
        osbap, on=["cusip", "date"], how="inner", suffix="_os"
    )
    return merged


def _resolve_keys(s1_col, os_col):
    """Return the actual column names after join with suffix='_os'."""
    if s1_col == os_col:
        return s1_col, f"{os_col}_os"
    return s1_col, os_col


def _column_pair_values(merged_df, s1_col, os_col):
    """Extract aligned, non-null numeric series for a column pair."""
    s1_key, os_key = _resolve_keys(s1_col, os_col)
    if s1_key not in merged_df.columns or os_key not in merged_df.columns:
        return None, None, s1_key, os_key

    # Extract as pandas Series for correlation/numeric ops
    pair = merged_df.select([s1_key, os_key]).to_pandas()
    s1_vals = pd.to_numeric(pair[s1_key], errors="coerce")
    os_vals = pd.to_numeric(pair[os_key], errors="coerce")
    mask = s1_vals.notna() & os_vals.notna()
    return s1_vals[mask], os_vals[mask], s1_key, os_key


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestMergeQuality:
    """Verify that the two datasets join properly."""

    def test_merge_not_empty(self, merged):
        assert len(merged) > 0, (
            "Inner join produced 0 rows. "
            "Check CUSIP format and month-end date alignment."
        )

    def test_merge_minimum_rows(self, merged):
        assert len(merged) >= MIN_MERGE_ROWS, (
            f"Only {len(merged)} merged rows (need >= {MIN_MERGE_ROWS}). "
            "The date overlap may be too small."
        )


# Parametrise one test per column pair
_continuous_cols = [
    (s1, os_, desc) for s1, os_, desc, cont in COLUMN_MAP if cont
]
_categorical_cols = [
    (s1, os_, desc) for s1, os_, desc, cont in COLUMN_MAP if not cont
]


class TestContinuousColumns:
    """For numeric analytics columns: correlation, median % diff, % within 1%."""

    @pytest.mark.parametrize(
        "s1_col,os_col,desc", _continuous_cols, ids=[d for _, _, d in _continuous_cols]
    )
    def test_correlation(self, merged, s1_col, os_col, desc):
        s1_vals, os_vals, s1_key, os_key = _column_pair_values(merged, s1_col, os_col)
        if s1_vals is None:
            pytest.fail(f"Column {s1_key} or {os_key} not in merged data")
        if len(s1_vals) == 0:
            pytest.fail(f"No non-null pairs for {desc}")
        corr = s1_vals.corr(os_vals)
        assert corr >= MIN_CORRELATION, (
            f"{desc}: correlation {corr:.4f} < {MIN_CORRELATION} "
            f"(n={len(s1_vals):,})"
        )

    @pytest.mark.parametrize(
        "s1_col,os_col,desc", _continuous_cols, ids=[d for _, _, d in _continuous_cols]
    )
    def test_median_pct_diff(self, merged, s1_col, os_col, desc):
        s1_vals, os_vals, s1_key, os_key = _column_pair_values(merged, s1_col, os_col)
        if s1_vals is None:
            pytest.fail(f"Column {s1_key} or {os_key} not in merged data")
        if len(s1_vals) == 0:
            pytest.fail(f"No non-null pairs for {desc}")
        diff = s1_vals - os_vals
        abs_pct = (diff / os_vals.replace(0, np.nan)).abs()
        median_pct = abs_pct.median()
        assert median_pct <= MAX_MEDIAN_ABS_PCT_DIFF, (
            f"{desc}: median |% diff| {median_pct:.4f} > {MAX_MEDIAN_ABS_PCT_DIFF} "
            f"(n={len(s1_vals):,})"
        )

    @pytest.mark.parametrize(
        "s1_col,os_col,desc", _continuous_cols, ids=[d for _, _, d in _continuous_cols]
    )
    def test_pct_within_tolerance(self, merged, s1_col, os_col, desc):
        s1_vals, os_vals, s1_key, os_key = _column_pair_values(merged, s1_col, os_col)
        if s1_vals is None:
            pytest.fail(f"Column {s1_key} or {os_key} not in merged data")
        if len(s1_vals) == 0:
            pytest.fail(f"No non-null pairs for {desc}")
        diff = s1_vals - os_vals
        abs_pct = (diff / os_vals.replace(0, np.nan)).abs()
        within = (abs_pct <= 0.01).mean() * 100
        assert within >= MIN_PCT_WITHIN_1PCT, (
            f"{desc}: only {within:.1f}% within 1% (need >= {MIN_PCT_WITHIN_1PCT}%) "
            f"(n={len(s1_vals):,})"
        )


class TestCategoricalColumns:
    """For discrete columns: exact-match rate."""

    @pytest.mark.parametrize(
        "s1_col,os_col,desc", _categorical_cols, ids=[d for _, _, d in _categorical_cols]
    )
    def test_exact_match_rate(self, merged, s1_col, os_col, desc):
        s1_vals, os_vals, s1_key, os_key = _column_pair_values(merged, s1_col, os_col)
        if s1_vals is None:
            pytest.fail(f"Column {s1_key} or {os_key} not in merged data")
        if len(s1_vals) == 0:
            pytest.fail(f"No non-null pairs for {desc}")
        match_rate = (s1_vals == os_vals).mean() * 100
        assert match_rate >= 90.0, (
            f"{desc}: only {match_rate:.1f}% exact match (need >= 90%) "
            f"(n={len(s1_vals):,})"
        )
