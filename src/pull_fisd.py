"""Pull FISD bond reference data from WRDS.

Saves:
    _data/pulled/fisd_issue.parquet
    _data/pulled/fisd_issuer.parquet
    _data/pulled/fisd_amt_out_hist.parquet
    _data/pulled/fisd_ratings_sp.parquet
    _data/pulled/fisd_ratings_moodys.parquet
    _data/pulled/fisd_redemption.parquet
"""

import logging
from pathlib import Path

from settings import config
from wrds_utils import wrds_connection, query_polars

logger = logging.getLogger(__name__)

DATA_DIR = Path(config("DATA_DIR"))
PULL_DIR = DATA_DIR / "pulled"
WRDS_USERNAME = config("WRDS_USERNAME")

ISSUE_COLUMNS = [
    "complete_cusip",
    "issue_id",
    "issue_name",
    "issuer_id",
    "foreign_currency",
    "coupon_type",
    "coupon",
    "convertible",
    "asset_backed",
    "rule_144a",
    "bond_type",
    "private_placement",
    "interest_frequency",
    "dated_date",
    "day_count_basis",
    "offering_date",
    "maturity",
    "principal_amt",
    "offering_amt",
]

ISSUER_COLUMNS = ["issuer_id", "country_domicile", "sic_code"]

QRY_ISSUE = f"""
    SELECT {', '.join(ISSUE_COLUMNS)}
    FROM   fisd.fisd_mergedissue
"""

QRY_ISSUER = f"""
    SELECT {', '.join(ISSUER_COLUMNS)}
    FROM   fisd.fisd_mergedissuer
"""

QRY_AMT_OUT_HIST = """
    SELECT *
    FROM   fisd.fisd_amt_out_hist
"""

QRY_RATINGS_SP = """
    SELECT issue_id, rating_date, rating
    FROM   fisd.fisd_ratings
    WHERE  rating_type = 'SPR'
"""

QRY_RATINGS_MOODYS = """
    SELECT issue_id, rating_date, rating
    FROM   fisd.fisd_ratings
    WHERE  rating_type = 'MR'
"""

QRY_REDEMPTION = """
    SELECT issue_id, callable
    FROM   fisd.fisd_mergedredemption
"""


def main():
    PULL_DIR.mkdir(parents=True, exist_ok=True)

    with wrds_connection(WRDS_USERNAME) as db:
        logger.info("Pulling FISD issue table...")
        issue_df = query_polars(db, QRY_ISSUE)
        logger.info("Pulled %d issue rows", len(issue_df))

        logger.info("Pulling FISD issuer table...")
        issuer_df = query_polars(db, QRY_ISSUER)
        logger.info("Pulled %d issuer rows", len(issuer_df))

        logger.info("Pulling FISD amount outstanding history...")
        amt_out_df = query_polars(db, QRY_AMT_OUT_HIST)
        logger.info("Pulled %d amt_out_hist rows", len(amt_out_df))

        logger.info("Pulling FISD S&P ratings...")
        sp_df = query_polars(db, QRY_RATINGS_SP)
        logger.info("Pulled %d S&P rating rows", len(sp_df))

        logger.info("Pulling FISD Moody's ratings...")
        mdy_df = query_polars(db, QRY_RATINGS_MOODYS)
        logger.info("Pulled %d Moody's rating rows", len(mdy_df))

        logger.info("Pulling FISD redemption (callable)...")
        redemp_df = query_polars(db, QRY_REDEMPTION)
        logger.info("Pulled %d redemption rows", len(redemp_df))

    issue_path = PULL_DIR / "fisd_issue.parquet"
    issuer_path = PULL_DIR / "fisd_issuer.parquet"

    issue_df.write_parquet(issue_path)
    issuer_df.write_parquet(issuer_path)

    logger.info("Wrote %s (%d rows)", issue_path, len(issue_df))
    logger.info("Wrote %s (%d rows)", issuer_path, len(issuer_df))

    amt_out_path = PULL_DIR / "fisd_amt_out_hist.parquet"
    amt_out_df.write_parquet(amt_out_path)
    logger.info("Wrote %s (%d rows)", amt_out_path, len(amt_out_df))

    sp_path = PULL_DIR / "fisd_ratings_sp.parquet"
    sp_df.write_parquet(sp_path)
    logger.info("Wrote %s (%d rows)", sp_path, len(sp_df))

    mdy_path = PULL_DIR / "fisd_ratings_moodys.parquet"
    mdy_df.write_parquet(mdy_path)
    logger.info("Wrote %s (%d rows)", mdy_path, len(mdy_df))

    redemp_path = PULL_DIR / "fisd_redemption.parquet"
    redemp_df.write_parquet(redemp_path)
    logger.info("Wrote %s (%d rows)", redemp_path, len(redemp_df))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s: %(message)s",
    )
    main()
