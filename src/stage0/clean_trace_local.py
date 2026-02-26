# -*- coding: utf-8 -*-
"""
clean_trace_local
=================
Read FISD-filtered TRACE data from local hive-partitioned parquet files,
apply the full Dick-Nielsen cleaning pipeline **month-by-month**, and write
hive-partitioned parquet output that stage1 expects.

Memory optimization: each month is read, cleaned, and written independently
so peak RSS stays proportional to one month of data (~2-4 GB) rather than
the full dataset (~30-50 GB).

No WRDS connection or multiprocessing required.
"""

from __future__ import annotations

import gc
import logging
import sys
import time
from datetime import date
from pathlib import Path
from typing import Iterator, Tuple

import numpy as np
import pandas as pd
import polars as pl
import pandas_market_calendars as mcal

# ---------------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

RUN_STAMP = pd.Timestamp.today().strftime("%Y%m%d")


# ---------------------------------------------------------------------------
# Reading partitioned parquet – month-by-month iterator
# ---------------------------------------------------------------------------

def _iter_partitions(
    base_dir: Path,
    start_date: str | None,
    end_date: str | None,
) -> Iterator[Tuple[int, int, pl.DataFrame]]:
    """
    Yield ``(year, month, pl.DataFrame)`` one partition at a time from
    ``base_dir/year=YYYY/month=MM/data.parquet``, keeping only partitions
    whose year/month overlap ``[start_date, end_date]``.
    """
    sd = date.fromisoformat(start_date) if start_date else date(1900, 1, 1)
    ed = date.fromisoformat(end_date) if end_date else date(2099, 12, 31)

    for year_dir in sorted(base_dir.glob("year=*")):
        year = int(year_dir.name.split("=")[1])
        for month_dir in sorted(year_dir.glob("month=*")):
            month = int(month_dir.name.split("=")[1])
            partition_start = date(year, month, 1)
            if month == 12:
                partition_end = date(year + 1, 1, 1)
            else:
                partition_end = date(year, month + 1, 1)
            if partition_start > ed or partition_end <= sd:
                continue
            pq_file = month_dir / "data.parquet"
            if pq_file.exists():
                df = pl.read_parquet(pq_file)
                logger.info(
                    "  Read %s  (%d rows)",
                    pq_file.relative_to(base_dir.parent),
                    len(df),
                )
                yield year, month, df


# ---------------------------------------------------------------------------
# Volume filter normalizer
# ---------------------------------------------------------------------------

def _normalize_volume_filter(v) -> Tuple[str, float]:
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return ("dollar", float(v))
    if isinstance(v, (tuple, list)) and len(v) == 2:
        kind, thr = v[0], v[1]
        kind = str(kind).strip().lower()
        if kind not in {"dollar", "par"}:
            raise ValueError("volume_filter kind must be 'dollar' or 'par'")
        return (kind, float(thr))
    raise ValueError(
        "volume_filter must be a number or a 2-tuple ('dollar'|'par', threshold)"
    )


# ---------------------------------------------------------------------------
# Time column normalizer (pandas – used before daily aggregation)
# ---------------------------------------------------------------------------

def _ensure_time_str(df: pd.DataFrame, col: str = "trd_exctn_tm") -> None:
    """
    Convert ``trd_exctn_tm`` in-place to string 'HH:MM:SS' if it isn't already.

    Handles three formats found in local parquet files:
      - int64  nanoseconds since midnight  (Enhanced / Standard)
      - datetime.time objects              (144A)
      - str 'HH:MM:SS'                    (already correct)
    """
    import datetime as _dt

    if col not in df.columns:
        return

    sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
    if sample is None:
        return

    # Already a string → nothing to do
    if isinstance(sample, str):
        return

    # datetime.time objects
    if isinstance(sample, _dt.time):
        df[col] = df[col].apply(
            lambda t: t.strftime("%H:%M:%S")
            if pd.notna(t) and isinstance(t, _dt.time)
            else np.nan
        )
        return

    # Numeric (int64 nanoseconds from parquet)
    if df[col].dtype.kind in ("i", "f", "u"):
        raw = df[col]
        has_na = raw.isna().any()
        secs = raw.fillna(-1).astype("int64") // 1_000_000_000
        hh = (secs // 3600).astype(str).str.zfill(2)
        mm = ((secs % 3600) // 60).astype(str).str.zfill(2)
        ss = (secs % 60).astype(str).str.zfill(2)
        df[col] = hh + ":" + mm + ":" + ss
        if has_na:
            df.loc[secs == -1, col] = np.nan
        return


# ---------------------------------------------------------------------------
# Polars helper: HH:MM:SS → seconds
# ---------------------------------------------------------------------------

def _hms_to_seconds_val(hms: str) -> float | None:
    """Convert 'HH:MM:SS' string to seconds since midnight."""
    try:
        parts = hms.split(":")
        return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Polars helper: trading-time filter
# ---------------------------------------------------------------------------

def _polars_time_filter(
    df: pl.DataFrame,
    start_s: float,
    end_s: float,
    keep_missing: bool,
) -> pl.DataFrame:
    """Filter a Polars DataFrame by trade execution time (``trd_exctn_tm``)."""
    col = "trd_exctn_tm"
    if col not in df.columns:
        return df

    dtype = df.schema[col]

    # Build an expression that maps the column to seconds since midnight
    if dtype == pl.String or dtype == pl.Utf8:
        parts = pl.col(col).str.split(":")
        tsec_expr = (
            parts.list.get(0).cast(pl.Int64, strict=False) * 3600
            + parts.list.get(1).cast(pl.Int64, strict=False) * 60
            + parts.list.get(2).cast(pl.Int64, strict=False)
        ).cast(pl.Float64)
    elif dtype == pl.Time:
        # pl.Time: extract hour/min/sec
        tsec_expr = (
            pl.col(col).dt.hour().cast(pl.Int64) * 3600
            + pl.col(col).dt.minute().cast(pl.Int64) * 60
            + pl.col(col).dt.second().cast(pl.Int64)
        ).cast(pl.Float64)
    elif dtype in (pl.Int64, pl.Float64, pl.UInt64):
        # nanoseconds since midnight
        tsec_expr = (pl.col(col).cast(pl.Float64) / 1_000_000_000)
    elif dtype == pl.Duration or str(dtype).startswith("Duration"):
        tsec_expr = pl.col(col).dt.total_seconds().cast(pl.Float64)
    else:
        return df

    df = df.with_columns(tsec_expr.alias("_tsec"))

    if end_s >= start_s:
        mask = (pl.col("_tsec") >= start_s) & (pl.col("_tsec") <= end_s)
    else:
        mask = (pl.col("_tsec") >= start_s) | (pl.col("_tsec") <= end_s)

    if keep_missing:
        mask = mask | pl.col("_tsec").is_null()
    else:
        mask = mask & pl.col("_tsec").is_not_null()

    return df.filter(mask).drop("_tsec")


# ---------------------------------------------------------------------------
# Polars helper: trading-calendar filter
# ---------------------------------------------------------------------------

def _polars_calendar_filter(
    df: pl.DataFrame,
    calendar_name: str,
    date_col: str,
    start_date: str,
    keep_missing: bool,
) -> pl.DataFrame:
    """Keep only rows whose ``date_col`` falls on a valid trading session."""
    if not calendar_name:
        return df

    cal = mcal.get_calendar(calendar_name)

    # Build a temporary Date column for comparison
    dtype = df.schema[date_col]
    if dtype == pl.Date:
        cast_expr = pl.col(date_col)
    else:
        cast_expr = pl.col(date_col).cast(pl.Date)

    df = df.with_columns(cast_expr.alias("_cal_date"))

    min_dt = df["_cal_date"].min()
    max_dt = df["_cal_date"].max()

    if min_dt is None or max_dt is None:
        return df.drop("_cal_date")

    sessions = cal.schedule(start_date=start_date, end_date=str(max_dt))
    valid_dates = pl.Series(
        "_vd", sorted(sessions.index.date), dtype=pl.Date
    )

    if keep_missing:
        result = df.filter(
            pl.col("_cal_date").is_in(valid_dates) | pl.col("_cal_date").is_null()
        )
    else:
        result = df.filter(pl.col("_cal_date").is_in(valid_dates))

    return result.drop("_cal_date")


# ---------------------------------------------------------------------------
# Core filter chain (shared between enhanced and standard/144a)
# ---------------------------------------------------------------------------

def _apply_filter_chain(
    trace: pl.DataFrame,
    fisd_off: pd.DataFrame,
    *,
    is_enhanced: bool,
    sort_cols: list[str],
    clean_fn,           # clean_trace_chunk or clean_trace_standard_chunk
    reversal_fn=None,   # clean_reversal (standard/144a only)
    decimal_shift_fn,
    bounce_back_fn,
    initial_error_fn,
    compute_metrics_fn,
    filter_by_calendar_fn,
    filter_by_trade_time_fn,
    filters: dict,
    volume_filter,
    trade_times,
    calendar_name: str | None,
    ds_params: dict | None,
    bb_params: dict | None,
    init_error_params: dict | None,
    clean_agency: bool,
    chunk_id: int = 0,
) -> pl.DataFrame | None:
    """
    Apply the full filter chain to one month of raw TRACE rows.

    Accepts a Polars DataFrame.  Heavy pandas-based Dick-Nielsen functions
    are called via pandas conversion at boundaries; simple scalar filters
    run natively in Polars.

    Returns daily-aggregated Polars DataFrame, or ``None`` if input is empty.
    """
    f = filters

    if len(trace) == 0:
        return None

    # =================================================================
    # GROUP 1 (pandas): Dick-Nielsen + Decimal Shift  [filters 1-2]
    # =================================================================
    trace_pd = trace.to_pandas()
    del trace
    gc.collect()

    trace_pd["rptd_pr"] = trace_pd["rptd_pr"].astype("float64").round(6)
    trace_pd = trace_pd.drop(columns=["index"], errors="ignore").reset_index(
        drop=True
    )

    # Ensure trd_exctn_dt is datetime64
    if "trd_exctn_dt" in trace_pd.columns:
        trace_pd["trd_exctn_dt"] = pd.to_datetime(trace_pd["trd_exctn_dt"])

    # Available sort columns
    sort_cols_avail = [c for c in sort_cols if c in trace_pd.columns]

    # ----- Filter 1: Dick-Nielsen -----
    if f.get("dick_nielsen", True):
        if is_enhanced:
            trace_pd = clean_fn(
                trace_pd, chunk_id=chunk_id, clean_agency=clean_agency
            )
        else:
            trace_pd = clean_fn(trace_pd, chunk_id=chunk_id)
        logger.info(
            "[chunk %03d] dick_nielsen: %d rows remaining",
            chunk_id,
            len(trace_pd),
        )
    if len(trace_pd) == 0:
        return None

    # Pre-decimal sort
    trace_pd = trace_pd.sort_values(
        sort_cols_avail, kind="mergesort", ignore_index=True
    )

    # ----- Filter 2: Decimal Shift -----
    if f.get("decimal_shift_corrector", True):
        _ds_defaults = dict(
            id_col="cusip_id",
            date_col="trd_exctn_dt",
            time_col="trd_exctn_tm",
            price_col="rptd_pr",
            factors=(0.1, 0.01, 10.0, 100.0),
            tol_pct_good=0.02,
            tol_abs_good=8.0,
            tol_pct_bad=0.05,
            low_pr=5.0,
            high_pr=300.0,
            anchor="rolling",
            window=5,
            improvement_frac=0.2,
            par_snap=True,
            par_band=15.0,
            output_type="cleaned",
        )
        _ds = {**_ds_defaults, **(ds_params or {})}
        trace_pd, n_replaced, _ = decimal_shift_fn(trace_pd, **_ds)
        logger.info(
            "[chunk %03d] decimal_shift: %d rows replaced", chunk_id, n_replaced
        )
    gc.collect()

    # =================================================================
    # GROUP 2 (Polars): Simple filters 3-6
    # =================================================================
    trace = pl.from_pandas(trace_pd)
    del trace_pd
    gc.collect()

    # ----- Filter 3: Trading Time -----
    if f.get("trading_time", False) and trade_times:
        start_s = _hms_to_seconds_val(trade_times[0])
        end_s = _hms_to_seconds_val(trade_times[1])
        if (
            start_s is not None
            and end_s is not None
            and "trd_exctn_tm" in trace.columns
        ):
            before = len(trace)
            trace = _polars_time_filter(
                trace, start_s, end_s, keep_missing=False
            )
            logger.info(
                "[chunk %03d] trading_time: kept %d (-%d)",
                chunk_id,
                len(trace),
                before - len(trace),
            )

    # ----- Filter 4: Trading Calendar -----
    if f.get("trading_calendar", True) and calendar_name:
        before = len(trace)
        trace = _polars_calendar_filter(
            trace, calendar_name, "trd_exctn_dt", "2002-07-01", keep_missing=False
        )
        logger.info(
            "[chunk %03d] calendar: kept %d (-%d)",
            chunk_id,
            len(trace),
            before - len(trace),
        )

    # ----- Filter 5: Price filters -----
    if f.get("price_filters", True):
        before = len(trace)
        trace = trace.filter(
            (pl.col("rptd_pr") > 0) & (pl.col("rptd_pr") <= 1000)
        )
        logger.info(
            "[chunk %03d] price_filters: kept %d (-%d)",
            chunk_id,
            len(trace),
            before - len(trace),
        )

    # ----- Filter 6: Volume filter -----
    trace = trace.with_columns(
        (pl.col("entrd_vol_qt") * pl.col("rptd_pr") / 100).alias("dollar_vol")
    )
    if f.get("volume_filter_toggle", True):
        vkind, vthr = _normalize_volume_filter(volume_filter)
        before = len(trace)
        if vkind == "dollar":
            trace = trace.filter(pl.col("dollar_vol") >= vthr)
        else:
            trace = trace.filter(pl.col("entrd_vol_qt") >= vthr)
        logger.info(
            "[chunk %03d] volume_filter[%s]: kept %d (-%d)",
            chunk_id,
            vkind,
            len(trace),
            before - len(trace),
        )

    if len(trace) == 0:
        return None

    # =================================================================
    # GROUP 3 (pandas): Bounce-back filter  [filter 7]
    # =================================================================
    trace_pd = trace.to_pandas()
    del trace
    gc.collect()

    # Ensure datetime for sort
    if "trd_exctn_dt" in trace_pd.columns:
        trace_pd["trd_exctn_dt"] = pd.to_datetime(trace_pd["trd_exctn_dt"])

    # Pre-BB sort
    trace_pd = trace_pd.sort_values(
        sort_cols_avail, kind="mergesort", ignore_index=True
    )

    if f.get("bounce_back_filter", True):
        _bb_defaults = dict(
            id_col="cusip_id",
            date_col="trd_exctn_dt",
            time_col="trd_exctn_tm",
            price_col="rptd_pr",
            threshold_abs=35.0,
            lookahead=5,
            max_span=5,
            window=5,
            back_to_anchor_tol=0.25,
            candidate_slack_abs=1.0,
            reassignment_margin_abs=5.0,
            use_unique_trailing_median=True,
            par_spike_heuristic=True,
            par_level=100.0,
            par_equal_tol=1e-8,
            par_min_run=3,
            par_cooldown_after_flag=2,
        )
        _bb = {**_bb_defaults, **(bb_params or {})}
        trace_pd = bounce_back_fn(trace_pd, **_bb)
        before = len(trace_pd)
        trace_pd = trace_pd[trace_pd["filtered_error"] == 0].copy()
        trace_pd.drop(
            ["delta_rptd_pr", "baseline_trailing", "filtered_error"],
            axis=1,
            inplace=True,
        )
        logger.info(
            "[chunk %03d] bounce_back: kept %d (-%d)",
            chunk_id,
            len(trace_pd),
            before - len(trace_pd),
        )
        gc.collect()

    # =================================================================
    # GROUP 4 (Polars): Simple filters 8-10
    # =================================================================
    trace = pl.from_pandas(trace_pd)
    del trace_pd
    gc.collect()

    # ----- Filter 8: Yield != Price -----
    if f.get("yld_price_filter", True):
        if "yld_pt" in trace.columns:
            before = len(trace)
            trace = trace.filter(
                (pl.col("rptd_pr") != pl.col("yld_pt"))
                | pl.col("yld_pt").is_null()
            )
            logger.info(
                "[chunk %03d] yld_price: kept %d (-%d)",
                chunk_id,
                len(trace),
                before - len(trace),
            )

    # ----- Filter 9: Amount outstanding vs volume -----
    fisd_off_pl = pl.from_pandas(fisd_off)
    trace = trace.join(fisd_off_pl, on="cusip_id", how="left")
    if f.get("amtout_volume_filter", True):
        before = len(trace)
        trace = trace.filter(
            pl.col("entrd_vol_qt") < pl.col("offering_amt") * 1000 * 0.50
        )
        logger.info(
            "[chunk %03d] amtout_volume: kept %d (-%d)",
            chunk_id,
            len(trace),
            before - len(trace),
        )

    # ----- Filter 10: Execution date <= maturity -----
    if f.get("trd_exe_mat_filter", True):
        before = len(trace)
        trace = trace.filter(pl.col("trd_exctn_dt") <= pl.col("maturity"))
        logger.info(
            "[chunk %03d] exctn_mat: kept %d (-%d)",
            chunk_id,
            len(trace),
            before - len(trace),
        )

    # =================================================================
    # GROUP 5 (pandas): Initial errors + daily aggregation  [filter 11+]
    # =================================================================
    trace_pd = trace.to_pandas()
    del trace
    gc.collect()

    # Ensure datetime
    if "trd_exctn_dt" in trace_pd.columns:
        trace_pd["trd_exctn_dt"] = pd.to_datetime(trace_pd["trd_exctn_dt"])

    # ----- Filter 11: Initial Price Errors -----
    if f.get("flag_initial_price_errors", True):
        _ie_defaults = dict(
            id_col="cusip_id",
            date_col="trd_exctn_dt",
            price_col="rptd_pr",
            abs_change=50.0,
            n_transactions=3,
        )
        _ie = {**_ie_defaults, **(init_error_params or {})}
        trace_pd = initial_error_fn(trace_pd, **_ie)
        before = len(trace_pd)
        trace_pd = trace_pd[trace_pd["initial_error_flag"] == 0].copy()
        trace_pd.drop(["initial_error_flag"], axis=1, inplace=True)
        logger.info(
            "[chunk %03d] init_price_error: kept %d (-%d)",
            chunk_id,
            len(trace_pd),
            before - len(trace_pd),
        )
        gc.collect()

    if len(trace_pd) == 0:
        return None

    # ----- Pre-aggregation: ensure trd_exctn_tm is string HH:MM:SS -----
    if "trd_exctn_tm" in trace_pd.columns:
        _ensure_time_str(trace_pd)

    # ----- Daily Aggregation -----
    all_data = compute_metrics_fn(trace_pd)
    del trace_pd
    gc.collect()

    return pl.from_pandas(all_data)


# ---------------------------------------------------------------------------
# Public API: clean_enhanced_trace
# ---------------------------------------------------------------------------

def clean_enhanced_trace(
    data_dir: Path,
    fisd_offamt_path: Path,
    fisd_universe_path: Path,
    start_date: str | None,
    end_date: str | None,
    out_dir: Path,
    data_type: str = "enhanced",
    **settings,
) -> None:
    """
    Read local FISD-filtered Enhanced TRACE parquet month-by-month,
    apply cleaning pipeline, write hive-partitioned daily output and
    FISD universe file for stage1.
    """
    import create_daily_enhanced_trace as emod
    from create_daily_enhanced_trace import (
        clean_trace_chunk,
        decimal_shift_corrector,
        flag_price_change_errors,
        flag_initial_price_errors,
        compute_trace_all_metrics,
        filter_by_calendar,
        filter_by_trade_time,
    )

    # Initialize module-level globals that cleaning functions rely on
    emod.audit_records = []
    emod.ct_audit_records = []
    emod.fisd_audit_records = []

    logger.info("=" * 70)
    logger.info("CLEAN ENHANCED TRACE (local parquet, month-by-month)")
    logger.info("=" * 70)

    # --- Read FISD offamt (small, fits in memory) ---
    base_dir = data_dir / "trace_enhanced_fisd"
    fisd_off = pd.read_parquet(fisd_offamt_path)
    logger.info("FISD offamt loaded: %d rows", len(fisd_off))

    # --- Sort columns ---
    sort_cols = [
        "cusip_id",
        "trd_exctn_dt",
        "trd_exctn_tm",
        "trd_rpt_dt",
        "trd_rpt_tm",
        "msg_seq_nb",
    ]

    # --- FILTER DEFAULTS ---
    FILTER_DEFAULTS = dict(
        dick_nielsen=True,
        decimal_shift_corrector=True,
        trading_time=False,
        trading_calendar=True,
        price_filters=True,
        volume_filter_toggle=True,
        bounce_back_filter=True,
        yld_price_filter=True,
        amtout_volume_filter=True,
        trd_exe_mat_filter=True,
        flag_initial_price_errors=True,
    )
    filters = {**FILTER_DEFAULTS, **(settings.get("filters") or {})}

    # --- Output directory ---
    out_sub = out_dir / "enhanced"
    out_sub.mkdir(parents=True, exist_ok=True)

    # --- Month-by-month processing ---
    month_count = 0
    total_rows = 0
    skipped_existing = 0
    start_t = time.time()

    for year, month, trace_pl in _iter_partitions(base_dir, start_date, end_date):
        month_count += 1

        # Skip months whose output already exists
        out_path = out_sub / f"year={year}" / f"month={month:02d}" / "data.parquet"
        if out_path.exists():
            logger.info("  Skipping %d-%02d (output already exists)", year, month)
            skipped_existing += 1
            del trace_pl
            continue

        logger.info(
            "-" * 50 + "\nProcessing %d-%02d (%d rows)",
            year,
            month,
            len(trace_pl),
        )

        # Row-level date filter in Polars
        if "trd_exctn_dt" in trace_pl.columns:
            # Ensure datetime type for comparison
            dt_dtype = trace_pl.schema["trd_exctn_dt"]
            if dt_dtype == pl.Date:
                if start_date:
                    trace_pl = trace_pl.filter(
                        pl.col("trd_exctn_dt")
                        >= pl.lit(start_date).str.to_date()
                    )
                if end_date:
                    trace_pl = trace_pl.filter(
                        pl.col("trd_exctn_dt")
                        <= pl.lit(end_date).str.to_date()
                    )
            else:
                # Datetime or other
                if start_date:
                    trace_pl = trace_pl.filter(
                        pl.col("trd_exctn_dt")
                        >= pl.lit(start_date).str.to_datetime()
                    )
                if end_date:
                    trace_pl = trace_pl.filter(
                        pl.col("trd_exctn_dt")
                        <= pl.lit(end_date).str.to_datetime()
                    )

        if len(trace_pl) == 0:
            logger.info("  Skipping %d-%02d (empty after date filter)", year, month)
            continue

        # Available sort columns for this partition
        sort_cols_avail = [c for c in sort_cols if c in trace_pl.columns]

        all_data = _apply_filter_chain(
            trace_pl,
            fisd_off,
            is_enhanced=True,
            sort_cols=sort_cols_avail,
            clean_fn=clean_trace_chunk,
            decimal_shift_fn=decimal_shift_corrector,
            bounce_back_fn=flag_price_change_errors,
            initial_error_fn=flag_initial_price_errors,
            compute_metrics_fn=compute_trace_all_metrics,
            filter_by_calendar_fn=filter_by_calendar,
            filter_by_trade_time_fn=filter_by_trade_time,
            filters=filters,
            volume_filter=settings.get("volume_filter", ("dollar", 10000)),
            trade_times=settings.get("trade_times"),
            calendar_name=settings.get("calendar_name", "NYSE"),
            ds_params=settings.get("ds_params"),
            bb_params=settings.get("bb_params"),
            init_error_params=settings.get("init_error_params"),
            clean_agency=settings.get("clean_agency", True),
            chunk_id=month_count,
        )

        if all_data is not None and len(all_data) > 0:
            # Write hive-partitioned output
            part_dir = out_sub / f"year={year}" / f"month={month:02d}"
            part_dir.mkdir(parents=True, exist_ok=True)
            out_path = part_dir / "data.parquet"
            all_data.write_parquet(out_path, compression="snappy")
            total_rows += len(all_data)
            logger.info(
                "  Wrote %s (%d rows)",
                out_path.relative_to(out_sub),
                len(all_data),
            )

        del trace_pl, all_data
        gc.collect()

    elapsed = round(time.time() - start_t, 1)
    logger.info(
        "Enhanced cleaning took %.1f seconds (%d months, %d new rows, %d skipped-existing)",
        elapsed,
        month_count,
        total_rows,
        skipped_existing,
    )

    if total_rows == 0 and skipped_existing == 0:
        raise RuntimeError("Enhanced cleaning produced no output rows.")
    if total_rows == 0 and skipped_existing > 0:
        logger.info("No new rows written (all %d months already existed).", skipped_existing)

    # --- Write FISD universe for stage1 ---
    fisd_universe = pd.read_parquet(fisd_universe_path)
    for dcol in ("dated_date", "offering_date", "maturity"):
        if dcol in fisd_universe.columns:
            fisd_universe[dcol] = pd.to_datetime(fisd_universe[dcol])
    fisd_out_path = out_sub / f"trace_enhanced_fisd_{RUN_STAMP}.parquet"
    fisd_universe.to_parquet(fisd_out_path, engine="pyarrow", compression="snappy")
    logger.info("Wrote %s  (%d rows)", fisd_out_path.name, len(fisd_universe))


# ---------------------------------------------------------------------------
# Public API: clean_standard_trace
# ---------------------------------------------------------------------------

def clean_standard_trace(
    data_dir: Path,
    fisd_offamt_path: Path,
    fisd_universe_path: Path,
    start_date: str | None,
    end_date: str | None,
    out_dir: Path,
    data_type: str = "standard",
    **settings,
) -> None:
    """
    Read local FISD-filtered Standard/144A TRACE parquet month-by-month,
    apply cleaning pipeline, write hive-partitioned daily output.
    """
    import create_daily_standard_trace as smod
    from create_daily_standard_trace import (
        clean_trace_standard_chunk,
        clean_reversal,
        decimal_shift_corrector,
        flag_price_change_errors,
        flag_initial_price_errors,
        compute_trace_all_metrics,
        filter_by_calendar,
        filter_by_trade_time,
    )

    # Initialize module-level globals that cleaning functions rely on
    smod.audit_records = []
    smod.ct_audit_records = []
    smod.fisd_audit_records = []

    label = data_type.upper()
    logger.info("=" * 70)
    logger.info("CLEAN %s TRACE (local parquet, month-by-month)", label)
    logger.info("=" * 70)

    # --- Read data ---
    base_dir = data_dir / f"trace_{data_type}_fisd"
    fisd_off = pd.read_parquet(fisd_offamt_path)
    logger.info("FISD offamt loaded: %d rows", len(fisd_off))

    # --- Sort columns ---
    sort_cols = ["cusip_id", "trd_exctn_dt", "trd_exctn_tm", "msg_seq_nb"]

    # --- FILTER DEFAULTS ---
    FILTER_DEFAULTS = dict(
        dick_nielsen=True,
        decimal_shift_corrector=True,
        trading_time=False,
        trading_calendar=True,
        price_filters=True,
        volume_filter_toggle=True,
        bounce_back_filter=True,
        yld_price_filter=True,
        amtout_volume_filter=True,
        trd_exe_mat_filter=True,
        flag_initial_price_errors=True,
    )
    filters = {**FILTER_DEFAULTS, **(settings.get("filters") or {})}

    # --- Output directory ---
    out_sub = out_dir / data_type
    out_sub.mkdir(parents=True, exist_ok=True)

    # --- Month-by-month processing ---
    month_count = 0
    total_rows = 0
    skipped_existing = 0
    start_t = time.time()

    for year, month, trace_pl in _iter_partitions(base_dir, start_date, end_date):
        month_count += 1

        # Skip months whose output already exists
        out_path = out_sub / f"year={year}" / f"month={month:02d}" / "data.parquet"
        if out_path.exists():
            logger.info("  Skipping %d-%02d (output already exists)", year, month)
            skipped_existing += 1
            del trace_pl
            continue

        logger.info(
            "-" * 50 + "\nProcessing %d-%02d (%d rows)",
            year,
            month,
            len(trace_pl),
        )

        # Row-level date filter in Polars
        if "trd_exctn_dt" in trace_pl.columns:
            dt_dtype = trace_pl.schema["trd_exctn_dt"]
            if dt_dtype == pl.Date:
                if start_date:
                    trace_pl = trace_pl.filter(
                        pl.col("trd_exctn_dt")
                        >= pl.lit(start_date).str.to_date()
                    )
                if end_date:
                    trace_pl = trace_pl.filter(
                        pl.col("trd_exctn_dt")
                        <= pl.lit(end_date).str.to_date()
                    )
            else:
                if start_date:
                    trace_pl = trace_pl.filter(
                        pl.col("trd_exctn_dt")
                        >= pl.lit(start_date).str.to_datetime()
                    )
                if end_date:
                    trace_pl = trace_pl.filter(
                        pl.col("trd_exctn_dt")
                        <= pl.lit(end_date).str.to_datetime()
                    )

        if len(trace_pl) == 0:
            logger.info("  Skipping %d-%02d (empty after date filter)", year, month)
            continue

        sort_cols_avail = [c for c in sort_cols if c in trace_pl.columns]

        all_data = _apply_filter_chain(
            trace_pl,
            fisd_off,
            is_enhanced=False,
            sort_cols=sort_cols_avail,
            clean_fn=clean_trace_standard_chunk,
            reversal_fn=clean_reversal,
            decimal_shift_fn=decimal_shift_corrector,
            bounce_back_fn=flag_price_change_errors,
            initial_error_fn=flag_initial_price_errors,
            compute_metrics_fn=compute_trace_all_metrics,
            filter_by_calendar_fn=filter_by_calendar,
            filter_by_trade_time_fn=filter_by_trade_time,
            filters=filters,
            volume_filter=settings.get("volume_filter", ("dollar", 10000)),
            trade_times=settings.get("trade_times"),
            calendar_name=settings.get("calendar_name", "NYSE"),
            ds_params=settings.get("ds_params"),
            bb_params=settings.get("bb_params"),
            init_error_params=settings.get("init_error_params"),
            clean_agency=settings.get("clean_agency", True),
            chunk_id=month_count,
        )

        if all_data is not None and len(all_data) > 0:
            part_dir = out_sub / f"year={year}" / f"month={month:02d}"
            part_dir.mkdir(parents=True, exist_ok=True)
            out_path = part_dir / "data.parquet"
            all_data.write_parquet(out_path, compression="snappy")
            total_rows += len(all_data)
            logger.info(
                "  Wrote %s (%d rows)",
                out_path.relative_to(out_sub),
                len(all_data),
            )

        del trace_pl, all_data
        gc.collect()

    elapsed = round(time.time() - start_t, 1)
    logger.info(
        "%s cleaning took %.1f seconds (%d months, %d new rows, %d skipped-existing)",
        label,
        elapsed,
        month_count,
        total_rows,
        skipped_existing,
    )

    if total_rows == 0 and skipped_existing == 0:
        raise RuntimeError(f"{label} cleaning produced no output rows.")
    if total_rows == 0 and skipped_existing > 0:
        logger.info("No new rows written (all %d months already existed).", skipped_existing)
