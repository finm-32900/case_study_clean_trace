"""Pull Fama-French SIC code classification files from Dartmouth.

Downloads FF17 and FF30 industry-to-SIC-range mappings, parses them,
and saves as parquet for offline consumption by Stage 1.

Saves:
    _data/pulled/ff17_sic_ranges.parquet
    _data/pulled/ff30_sic_ranges.parquet
"""

import logging
from io import BytesIO
from pathlib import Path
from zipfile import ZipFile

import polars as pl
import requests

from settings import config

logger = logging.getLogger(__name__)

DATA_DIR = Path(config("DATA_DIR"))
PULL_DIR = DATA_DIR / "pulled"

FF_SOURCES = {
    "ff17": {
        "url": "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/Siccodes17.zip",
        "zip_member": "Siccodes17.txt",
        "parquet": "ff17_sic_ranges.parquet",
    },
    "ff30": {
        "url": "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/Siccodes30.zip",
        "zip_member": "Siccodes30.txt",
        "parquet": "ff30_sic_ranges.parquet",
    },
}


def _parse_ff_sic_file(content: str) -> list[dict]:
    """Parse a Fama-French SIC code text file into a list of range records.

    Each record has: ind_num, ind_name, sic_start, sic_end.
    """
    industries = []
    current_ind_num = None
    current_ind_name = None

    for line in content.split("\n"):
        line = line.strip()
        if not line:
            continue

        parts = line.split()
        if parts and parts[0].isdigit() and len(parts[0]) <= 2:
            current_ind_num = int(parts[0])
            if len(parts) >= 2:
                current_ind_name = parts[1]
            continue

        if current_ind_num is not None and "-" in line:
            range_str = parts[0] if parts else ""
            if "-" in range_str:
                try:
                    start_str, end_str = range_str.split("-")
                    industries.append(
                        {
                            "ind_num": current_ind_num,
                            "ind_name": current_ind_name,
                            "sic_start": int(start_str),
                            "sic_end": int(end_str),
                        }
                    )
                except (ValueError, IndexError):
                    continue

    return industries


def main():
    PULL_DIR.mkdir(parents=True, exist_ok=True)

    for name, info in FF_SOURCES.items():
        logger.info("Downloading %s from %s ...", name, info["url"])
        resp = requests.get(info["url"], timeout=30)
        resp.raise_for_status()

        with ZipFile(BytesIO(resp.content)) as z:
            with z.open(info["zip_member"]) as f:
                content = f.read().decode("utf-8", errors="ignore")

        records = _parse_ff_sic_file(content)
        df = pl.DataFrame(records)

        out_path = PULL_DIR / info["parquet"]
        df.write_parquet(out_path)
        logger.info("Wrote %s (%d SIC ranges)", out_path, len(df))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s: %(message)s",
    )
    main()
