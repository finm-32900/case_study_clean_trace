"""Shared utilities for incremental hive-partitioned cleaning steps."""

import logging
from pathlib import Path

import polars as pl

from pull_utils import existing_partitions, partition_path

logger = logging.getLogger(__name__)


def partitions_to_clean(
    input_dir: Path,
    input_dataset: str,
    output_dataset: str,
    output_dir: Path | None = None,
) -> list[tuple[int, int]]:
    """Return (year, month) tuples that exist in input but not in output.

    Unlike pulls, we do NOT re-clean the current month -- cleaning is
    deterministic given the local raw data.

    Parameters
    ----------
    input_dir : base directory for the raw input partitions
    input_dataset : name of the input dataset (e.g. "trace_enhanced")
    output_dataset : name of the output dataset (e.g. "trace_enhanced_fisd")
    output_dir : base directory for the output partitions (defaults to input_dir)
    """
    if output_dir is None:
        output_dir = input_dir
    input_parts = existing_partitions(input_dir, input_dataset)
    output_parts = existing_partitions(output_dir, output_dataset)
    return sorted(input_parts - output_parts)


def read_partition(
    base_dir: Path, dataset: str, year: int, month: int
) -> pl.DataFrame:
    """Read a single hive-partition parquet file as a Polars DataFrame."""
    path = partition_path(base_dir, dataset, year, month)
    return pl.read_parquet(path)


def write_clean_partition(
    df: pl.DataFrame,
    base_dir: Path,
    dataset: str,
    year: int,
    month: int,
) -> Path:
    """Write a Polars DataFrame to a single hive-partition parquet file.

    Creates directories as needed. Returns the path written.
    """
    out_path = partition_path(base_dir, dataset, year, month)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out_path)
    logger.info("Wrote %d rows to %s", len(df), out_path)
    return out_path
