"""WRDS connection utilities.

Provides a context-managed WRDS connection and a helper that
executes SQL, converts the pandas DataFrame returned by wrds to
a polars DataFrame, and retries on transient errors.
"""

import logging
import time
from contextlib import contextmanager

import polars as pl
import wrds

logger = logging.getLogger(__name__)


@contextmanager
def wrds_connection(username: str):
    """Context manager for a WRDS connection.

    Usage:
        with wrds_connection("jmbejara") as db:
            df = query_polars(db, "SELECT 1 AS x")
    """
    db = wrds.Connection(wrds_username=username)
    try:
        yield db
    finally:
        db.close()
        logger.info("WRDS connection closed.")


def query_polars(
    db,
    sql: str,
    params: dict | None = None,
    *,
    max_retries: int = 3,
    base_sleep: float = 2.0,
) -> pl.DataFrame:
    """Execute SQL via wrds.Connection.raw_sql and return a polars DataFrame.

    The wrds library returns pandas; we convert immediately to polars.
    Includes retry logic for transient connection errors.
    """
    for attempt in range(max_retries):
        try:
            pdf = db.raw_sql(sql, params=params)
            return pl.from_pandas(pdf)
        except Exception as exc:
            if attempt < max_retries - 1:
                wait = base_sleep * (2**attempt)
                logger.warning(
                    "WRDS query failed (attempt %d/%d), retrying in %.1fs: %s",
                    attempt + 1,
                    max_retries,
                    wait,
                    exc,
                )
                time.sleep(wait)
            else:
                raise
