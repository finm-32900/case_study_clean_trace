"""Pull raw TRACE Enhanced data from WRDS, month by month.

Saves hive-partitioned parquet:
    _data/trace_enhanced/year=YYYY/month=MM/data.parquet

Incremental: skips months that already exist locally (except current month).
"""

import logging
import time
from datetime import date
from pathlib import Path

from settings import config, get_start_date, get_end_date
from wrds_utils import wrds_connection, query_polars
from pull_utils import existing_partitions, months_to_pull, write_partition

logger = logging.getLogger(__name__)

DATA_DIR = Path(config("DATA_DIR"))
WRDS_USERNAME = config("WRDS_USERNAME")

DATASET = "trace_enhanced"
TABLE = "trace.trace_enhanced"
DEFAULT_START_DATE = date(2002, 7, 1)
START_DATE = get_start_date() or DEFAULT_START_DATE
END_DATE = get_end_date()  # None means "today" (handled by months_to_pull)

COLUMNS = [
    "cusip_id",
    "bond_sym_id",
    "trd_exctn_dt",
    "trd_exctn_tm",
    "days_to_sttl_ct",
    "lckd_in_ind",
    "wis_fl",
    "sale_cndtn_cd",
    "msg_seq_nb",
    "trc_st",
    "trd_rpt_dt",
    "trd_rpt_tm",
    "entrd_vol_qt",
    "rptd_pr",
    "yld_pt",
    "asof_cd",
    "orig_msg_seq_nb",
    "rpt_side_cd",
    "cntra_mp_id",
]

SQL_TEMPLATE = """
    SELECT {columns}
    FROM   {table}
    WHERE  trd_exctn_dt >= %(start)s
      AND  trd_exctn_dt <  %(end)s
"""


def pull_month(db, year: int, month: int) -> "pl.DataFrame":
    """Pull a single month of Enhanced TRACE data."""
    import polars as pl

    start = date(year, month, 1)
    if month == 12:
        end = date(year + 1, 1, 1)
    else:
        end = date(year, month + 1, 1)

    sql = SQL_TEMPLATE.format(
        columns=", ".join(COLUMNS),
        table=TABLE,
    )
    df = query_polars(db, sql, params={"start": start, "end": end})
    logger.info("Month %d-%02d: pulled %d rows", year, month, len(df))
    return df


def main():
    existing = existing_partitions(DATA_DIR, DATASET)
    logger.info("Found %d existing partitions for %s", len(existing), DATASET)

    to_pull = months_to_pull(START_DATE, existing, end_date=END_DATE, base_dir=DATA_DIR, dataset=DATASET)
    logger.info("Need to pull %d months", len(to_pull))

    if not to_pull:
        logger.info("All partitions up to date. Nothing to do.")
        return

    with wrds_connection(WRDS_USERNAME) as db:
        for year, month in to_pull:
            df = pull_month(db, year, month)
            write_partition(df, DATA_DIR, DATASET, year, month)
            if len(df) == 0:
                logger.info("Month %d-%02d: no data, wrote empty partition", year, month)
            time.sleep(1)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s: %(message)s",
    )
    main()
