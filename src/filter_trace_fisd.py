"""Filter raw TRACE data to only CUSIPs present in the FISD universe.

For each TRACE dataset (enhanced, standard, 144a):
    Reads:  _data/pulled/{dataset}/year=YYYY/month=MM/data.parquet
    Writes: _data/{dataset}_fisd/year=YYYY/month=MM/data.parquet

Only keeps rows whose cusip_id is in the FISD universe.
Incremental: skips months already written.

Can process a single dataset via CLI:
    ipython filter_trace_fisd.py -- --DATASET=trace_enhanced
Or all three datasets if no --DATASET is given.
"""

import logging
import sys
from pathlib import Path

import polars as pl

from settings import config
from clean_utils import partitions_to_clean, read_partition, write_clean_partition

logger = logging.getLogger(__name__)

DATA_DIR = Path(config("DATA_DIR"))
PULL_DIR = DATA_DIR / "pulled"

ALL_DATASETS = ["trace_enhanced", "trace_standard", "trace_144a"]


def _parse_dataset_arg() -> str | None:
    """Parse --DATASET=name from command line, if present."""
    for arg in sys.argv:
        if arg.startswith("--DATASET="):
            return arg.split("=", 1)[1]
    return None


def filter_dataset_fisd(
    dataset: str,
    fisd_cusips: list[str],
    input_dir: Path,
    output_dir: Path,
) -> None:
    """Filter one TRACE dataset to FISD universe CUSIPs, month by month."""
    output_dataset = f"{dataset}_fisd"

    to_clean = partitions_to_clean(input_dir, dataset, output_dataset, output_dir=output_dir)
    logger.info("[%s] %d months to filter", dataset, len(to_clean))

    if not to_clean:
        logger.info("[%s] All months already filtered. Nothing to do.", dataset)
        return

    for year, month in to_clean:
        df = read_partition(input_dir, dataset, year, month)
        n_before = len(df)

        df = df.filter(pl.col("cusip_id").is_in(fisd_cusips))
        n_after = len(df)

        logger.info(
            "[%s] %d-%02d: %d -> %d rows (-%d)",
            dataset,
            year,
            month,
            n_before,
            n_after,
            n_before - n_after,
        )

        write_clean_partition(df, output_dir, output_dataset, year, month)
        if n_after == 0:
            logger.info(
                "[%s] %d-%02d: no rows after FISD filter, wrote empty partition",
                dataset,
                year,
                month,
            )


def main():
    fisd_universe = pl.read_parquet(DATA_DIR / "fisd_universe.parquet")
    fisd_cusips = fisd_universe["complete_cusip"].to_list()
    logger.info("FISD universe: %d unique CUSIPs", len(fisd_cusips))

    dataset_arg = _parse_dataset_arg()
    datasets = [dataset_arg] if dataset_arg else ALL_DATASETS

    for dataset in datasets:
        input_path = PULL_DIR / dataset
        if not input_path.exists():
            logger.warning(
                "Skipping %s -- input directory does not exist", dataset
            )
            continue
        filter_dataset_fisd(dataset, fisd_cusips, input_dir=PULL_DIR, output_dir=DATA_DIR)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s: %(message)s",
    )
    main()
