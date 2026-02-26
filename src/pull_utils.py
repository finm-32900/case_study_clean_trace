"""Shared utilities for incremental hive-partitioned data pulls."""

import logging
from datetime import date, datetime
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)


def partition_path(base_dir: Path, dataset: str, year: int, month: int) -> Path:
    """Return the path for a single partition file.

    Example: _data/trace_enhanced/year=2023/month=11/data.parquet
    """
    return base_dir / dataset / f"year={year}" / f"month={month:02d}" / "data.parquet"


def existing_partitions(base_dir: Path, dataset: str) -> set[tuple[int, int]]:
    """Scan the filesystem and return a set of (year, month) tuples
    for partitions that already exist.
    """
    dataset_dir = base_dir / dataset
    found = set()
    if not dataset_dir.exists():
        return found
    for parquet_file in dataset_dir.glob("year=*/month=*/data.parquet"):
        parts = parquet_file.parts
        year_part = [p for p in parts if p.startswith("year=")]
        month_part = [p for p in parts if p.startswith("month=")]
        if year_part and month_part:
            y = int(year_part[0].split("=")[1])
            m = int(month_part[0].split("=")[1])
            found.add((y, m))
    return found


def _partition_written_today(base_dir: Path, dataset: str, year: int, month: int) -> bool:
    """Return True if the partition file exists and was last modified today."""
    path = partition_path(base_dir, dataset, year, month)
    if not path.exists():
        return False
    mtime = datetime.fromtimestamp(path.stat().st_mtime).date()
    return mtime == date.today()


def months_to_pull(
    start_date: date,
    existing: set[tuple[int, int]],
    end_date: date | None = None,
    base_dir: Path | None = None,
    dataset: str | None = None,
) -> list[tuple[int, int]]:
    """Return the list of (year, month) tuples that need to be pulled.

    Rules:
    - Skip any month already in `existing` UNLESS it is the current month
      (current month is always re-pulled because it may have new data).
    - For the current month, skip if the partition was already written today
      (avoids redundant WRDS queries on repeated runs the same day).
    - Range is from start_date to end_date (defaults to today).
    """
    if end_date is None:
        end_date = date.today()

    current_year, current_month = end_date.year, end_date.month
    result = []
    y, m = start_date.year, start_date.month

    while (y, m) <= (end_date.year, end_date.month):
        is_current_month = y == current_year and m == current_month
        if (y, m) not in existing:
            result.append((y, m))
        elif is_current_month:
            if base_dir and dataset and _partition_written_today(base_dir, dataset, y, m):
                pass  # already pulled today, skip
            else:
                result.append((y, m))
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1

    return result


def enforce_schema(
    df: pl.DataFrame,
    schema: dict[str, pl.DataType],
) -> pl.DataFrame:
    """Cast columns to their expected types.

    WRDS sometimes returns all-string pandas DataFrames depending on the
    database driver version.  This function normalises the column types so
    that every partition written to disk has a consistent schema.

    Only columns present in both *df* and *schema* are cast; extra columns
    on either side are silently ignored.
    """
    casts = []
    for col, target in schema.items():
        if col not in df.columns or df.schema[col] == target:
            continue
        src = df.schema[col]
        # All-null columns can be cast directly; string parsing would fail.
        if df[col].is_null().all():
            casts.append(pl.col(col).cast(target, strict=False))
        elif target == pl.Date and src == pl.String:
            casts.append(pl.col(col).str.to_date(strict=False))
        elif target == pl.Time and src == pl.String:
            casts.append(pl.col(col).str.to_time(strict=False))
        else:
            casts.append(pl.col(col).cast(target, strict=False))
    if casts:
        df = df.with_columns(casts)
    return df


def write_partition(
    df: pl.DataFrame,
    base_dir: Path,
    dataset: str,
    year: int,
    month: int,
) -> Path:
    """Write a polars DataFrame to a single hive-partition parquet file.

    Creates directories as needed. Returns the path written.
    """
    out_path = partition_path(base_dir, dataset, year, month)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out_path)
    logger.info("Wrote %d rows to %s", len(df), out_path)
    return out_path
