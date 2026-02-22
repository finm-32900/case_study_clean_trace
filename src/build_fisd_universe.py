"""Build the FISD bond universe from already-pulled FISD reference data.

Applies configurable screens (FISD_PARAMS) to filter the issue-level table
down to the set of bonds used in the TRACE cleaning pipeline.

Reads:
    _data/pulled/fisd_issue.parquet
    _data/pulled/fisd_issuer.parquet

Writes:
    _data/fisd_universe.parquet         (filtered FISD issue table, all columns)
    _data/fisd_universe_offamt.parquet  (cusip_id, offering_amt, maturity only)
"""

import logging
from pathlib import Path

import polars as pl

from settings import config, FISD_PARAMS

logger = logging.getLogger(__name__)

DATA_DIR = Path(config("DATA_DIR"))


def _log_step(fisd: pl.DataFrame, step: str) -> None:
    logger.info("[FISD] %-40s  %d rows", step, len(fisd))


def build_fisd_universe(
    issue_path: Path,
    issuer_path: Path,
    params: dict | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Apply FISD screens and return (fisd, fisd_offamt).

    Parameters
    ----------
    issue_path : Path to fisd_issue.parquet
    issuer_path : Path to fisd_issuer.parquet
    params : optional overrides merged onto FISD_PARAMS

    Returns
    -------
    fisd : filtered issue-level DataFrame (all columns)
    fisd_offamt : DataFrame with cusip_id, offering_amt, maturity
    """
    p = {**FISD_PARAMS, **(params or {})}

    issue = pl.read_parquet(issue_path)
    issuer = pl.read_parquet(issuer_path)
    fisd = issue.join(issuer, on="issuer_id", how="left")

    _log_step(fisd, "start")

    # 1) USD only
    if p["currency_usd_only"]:
        fisd = fisd.filter(pl.col("foreign_currency") == "N")
        _log_step(fisd, "USD currency")

    # 2) Fixed rate (exclude variable coupon)
    if p["fixed_rate_only"]:
        fisd = fisd.filter(pl.col("coupon_type") != "V")
        _log_step(fisd, "fixed rate")

    # 3) Non-convertible
    if p["non_convertible_only"]:
        fisd = fisd.filter(pl.col("convertible") == "N")
        _log_step(fisd, "non convertible")

    # 4) Non-asset-backed
    if p["non_asset_backed_only"]:
        fisd = fisd.filter(pl.col("asset_backed") == "N")
        _log_step(fisd, "non asset backed")

    # 5) Exclude certain bond types
    if p["exclude_bond_types"]:
        fisd = fisd.filter(~pl.col("bond_type").is_in(p["excluded_bond_types"]))
        _log_step(fisd, "exclude bond types")

    # 6) Valid coupon frequency (cast to int, fill null with -1)
    fisd = fisd.with_columns(
        pl.col("interest_frequency")
        .cast(pl.Int64, strict=False)
        .fill_null(-1)
        .alias("interest_frequency")
    )
    if p["valid_coupon_frequency_only"]:
        fisd = fisd.filter(
            ~pl.col("interest_frequency").is_in(p["invalid_coupon_freq"])
        )
        _log_step(fisd, "valid coupon frequency")

    # 7) Require accrual fields (non-null)
    if p["require_accrual_fields"]:
        fisd = fisd.with_columns(
            pl.col("offering_date").cast(pl.Date, strict=False),
            pl.col("dated_date").cast(pl.Date, strict=False),
        )
        req_cols = [
            "offering_date",
            "dated_date",
            "interest_frequency",
            "day_count_basis",
            "coupon_type",
            "coupon",
        ]
        fisd = fisd.drop_nulls(subset=req_cols)
        _log_step(fisd, "complete accrual fields")

    # 8) principal_amt == 1000
    if p["principal_amt_eq_1000_only"]:
        fisd = fisd.filter(pl.col("principal_amt") == 1000)
        _log_step(fisd, "principal_amt == 1000")

    # 9) Exclude equity-linked / index-linked
    if p["exclude_equity_index_linked"]:
        pattern = (
            r"(?i)EQUITY\-LINKED|EQUITY LINKED|EQUITYLINKED"
            r"|INDEX\-LINKED|INDEX LINKED|INDEXLINKED"
        )
        fisd = fisd.filter(~pl.col("issue_name").str.contains(pattern))
        _log_step(fisd, "exclude equity/index linked")

    # 10) Tenor >= minimum years
    if p["enforce_tenor_min"]:
        fisd = fisd.with_columns(
            pl.col("maturity").cast(pl.Date, strict=False),
            pl.col("offering_date").cast(pl.Date, strict=False),
        )
        fisd = fisd.with_columns(
            ((pl.col("maturity") - pl.col("offering_date")).dt.total_days() / 365.25)
            .alias("tenor")
        )
        fisd = fisd.filter(pl.col("tenor") >= p["tenor_min_years"])
        _log_step(fisd, f"tenor >= {p['tenor_min_years']} year(s)")

    # Build offamt table (cusip_id, offering_amt, maturity) for downstream filters
    fisd_offamt = fisd.select(
        pl.col("complete_cusip").alias("cusip_id"),
        "offering_amt",
        "maturity",
    )

    return fisd, fisd_offamt


def main():
    issue_path = DATA_DIR / "pulled" / "fisd_issue.parquet"
    issuer_path = DATA_DIR / "pulled" / "fisd_issuer.parquet"

    fisd, fisd_offamt = build_fisd_universe(issue_path, issuer_path)

    out_universe = DATA_DIR / "fisd_universe.parquet"
    out_offamt = DATA_DIR / "fisd_universe_offamt.parquet"

    fisd.write_parquet(out_universe)
    fisd_offamt.write_parquet(out_offamt)

    logger.info("Wrote %s (%d rows)", out_universe, len(fisd))
    logger.info("Wrote %s (%d rows)", out_offamt, len(fisd_offamt))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s: %(message)s",
    )
    main()
