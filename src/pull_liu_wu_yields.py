"""Pull Liu-Wu zero-coupon Treasury yields from Google Sheets.

Downloads the Liu-Wu yield curve data, transforms it to match the FRED-style
column naming convention used by Stage 1, and saves as parquet.

Saves:
    _data/pulled/liu_wu_yields.parquet
"""

from io import BytesIO
from pathlib import Path

import pandas as pd
import requests

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
PULL_DIR = DATA_DIR / "pulled"

LIU_WU_URL = "https://docs.google.com/spreadsheets/d/11HsxLl_u2tBNt3FyN5iXGsIKLwxvVz7t/edit?usp=sharing"

# Map Liu-Wu maturity labels to FRED-style column names
MATURITY_MAP = {
    "12 m": "oneyr",
    "24 m": "twoyr",
    "60 m": "fiveyr",
    "84 m": "sevyr",
    "120 m": "tenyr",
    "240 m": "twentyr",
    "360 m": "thirtyr",
}


def pull_liu_wu_yields(
    url: str = LIU_WU_URL,
    start_date: str = "2000-01-31",
) -> pd.DataFrame:
    """Download Liu-Wu yields from Google Sheets and return a cleaned DataFrame.

    Returns columns: trd_exctn_dt, oneyr, twoyr, fiveyr, sevyr, tenyr, twentyr, thirtyr
    Yields are converted from percentage points to decimals (e.g. 3.5% -> 0.035).
    Daily-resampled with forward fill.
    """
    # Convert sharing URL to export URL
    if "/edit?" in url or "/edit#" in url:
        sheet_id = url.split("/d/")[1].split("/")[0]
        export_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx&id={sheet_id}"
    else:
        export_url = url

    print(f"Downloading Liu-Wu yields from Google Sheets...")
    response = requests.get(export_url, timeout=60)
    response.raise_for_status()

    df = pd.read_excel(BytesIO(response.content), header=8)
    df.columns = df.columns.str.strip()

    date_col = df.columns[0]
    result = pd.DataFrame()
    result["trd_exctn_dt"] = pd.to_datetime(
        df[date_col].astype(str), format="%Y%m%d", errors="coerce"
    )

    for mat_label, out_col in MATURITY_MAP.items():
        if mat_label not in df.columns:
            raise ValueError(f"Expected maturity '{mat_label}' not found in Liu-Wu data")
        result[out_col] = pd.to_numeric(df[mat_label], errors="coerce") / 100.0

    start = pd.to_datetime(start_date)
    result = result[result["trd_exctn_dt"] >= start].copy()

    # Resample daily with forward fill
    result = result.set_index("trd_exctn_dt")
    result = result.resample("D").last().ffill().reset_index()
    result = result.dropna(subset=["trd_exctn_dt"])
    result = result.sort_values("trd_exctn_dt").reset_index(drop=True)

    return result


def load_liu_wu_yields(data_dir=PULL_DIR) -> pd.DataFrame:
    """Load pre-pulled Liu-Wu yields from parquet."""
    path = Path(data_dir) / "liu_wu_yields.parquet"
    return pd.read_parquet(path)


if __name__ == "__main__":
    PULL_DIR.mkdir(parents=True, exist_ok=True)
    path = PULL_DIR / "liu_wu_yields.parquet"

    if path.exists():
        print(f"Already exists: {path} — skipping pull.")
    else:
        df = pull_liu_wu_yields()
        df.to_parquet(path, index=False)
        print(f"Saved {len(df)} rows to {path}")
