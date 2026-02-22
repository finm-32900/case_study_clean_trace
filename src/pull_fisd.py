"""Pull FISD bond reference data from WRDS.

Saves:
    _data/pulled/fisd_issue.parquet
    _data/pulled/fisd_issuer.parquet
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


def main():
    PULL_DIR.mkdir(parents=True, exist_ok=True)

    with wrds_connection(WRDS_USERNAME) as db:
        logger.info("Pulling FISD issue table...")
        issue_df = query_polars(db, QRY_ISSUE)
        logger.info("Pulled %d issue rows", len(issue_df))

        logger.info("Pulling FISD issuer table...")
        issuer_df = query_polars(db, QRY_ISSUER)
        logger.info("Pulled %d issuer rows", len(issuer_df))

    issue_path = PULL_DIR / "fisd_issue.parquet"
    issuer_path = PULL_DIR / "fisd_issuer.parquet"

    issue_df.write_parquet(issue_path)
    issuer_df.write_parquet(issuer_path)

    logger.info("Wrote %s (%d rows)", issue_path, len(issue_df))
    logger.info("Wrote %s (%d rows)", issuer_path, len(issuer_df))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s: %(message)s",
    )
    main()
