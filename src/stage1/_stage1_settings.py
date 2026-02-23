# -*- coding: utf-8 -*-
"""
Stage 1 Configuration Settings
================================
Central configuration for Stage 1 TRACE processing pipeline.
Edit this file to customize your processing parameters.

Author: Alex Dickerson
Created: 2025-11-17
"""

from __future__ import annotations
from pathlib import Path
import os
import sys

# Import shared configuration from root-level config.py
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import WRDS_USERNAME, OUTPUT_FORMAT, TRACE_MEMBERS
import decouple

# ============================================================================
# USER CONFIGURATION - EDIT THESE VALUES
# ============================================================================

# --- Paths Configuration ---
# ROOT_PATH is the parent directory containing both stage0/ and stage1/ folders
#
# Option 1: AUTO-DETECT (recommended - leave blank or use "")
# The code will automatically detect ROOT_PATH from the current working directory.
# If you run the script from ~/proj/stage1, ROOT_PATH will be set to ~/proj
ROOT_PATH = ""  # Auto-detect from current working directory

# Option 2: MANUAL OVERRIDE (uncomment and edit if auto-detect doesn't work)
# Uncomment ONE of the following lines if you need to manually specify ROOT_PATH:
# ROOT_PATH = Path("~/proj").expanduser()                          # Linux/Mac/WRDS
# ROOT_PATH = Path("C:\\Users\\YourName\\Documents\\trace_data")   # Windows
# ROOT_PATH = Path("/Users/yourname/Documents/trace_data")         # Mac

# --- Stage0 Input Configuration ---
# STAGE0_DATE_STAMP will be auto-detected from stage0 parquet files (see DERIVED PATHS section)
# NOTE: TRACE_MEMBERS is imported from config.py (shared across all stages)

# --- Execution Settings ---
# Read N_CORES from .env if set, otherwise auto-detect
_n_cores_env = decouple.config("N_CORES", default=None)
if _n_cores_env is not None:
    N_CORES = int(_n_cores_env)
else:
    import multiprocessing
    N_CORES = multiprocessing.cpu_count()

N_CHUNKS = 10      # Number of chunks for parallel operations

# --- Date Filter ---
DATE_CUT_OFF = "2025-03-31"  # Only include data on/before this date

# --- Output Settings ---
# OUTPUT_FORMAT is imported from shared config.py
# NOTE: Stage 1 always generates comprehensive reports and figures.
# These outputs are essential for data quality assessment and cannot be disabled.

# ============================================================================
# ULTRA DISTRESSED FILTER CONFIGURATION
# ============================================================================

ULTRA_DISTRESSED_CONFIG = {
    'price_col': 'pr',

    # Intraday inconsistency (Step 4)
    'intraday_range_threshold': 0.75,  # % Within Day Move for Flag
    'intraday_price_threshold': 20,    # Kicks in below this % of par

    # Anomaly detection
    'ultra_low_threshold': 0.10,
    'min_normal_price_ratio': 3.0,

    # Plateau detection
    'plateau_ultra_low_threshold': 0.15,
    'min_plateau_days': 2,

    # Round numbers
    'suspicious_round_numbers': [0.001, 0.01, 0.05, 0.10, 0.25, 0.50, 1.00],

    # Intraday consistency (high spike detection)
    'price_cols': ['prc_hi', 'prc_lo'],
    'high_spike_threshold': 5.0,
    'min_spike_ratio': 3.0,
    'recovery_ratio': 2.0,
    'verbose': True,

    # Chunking settings
    'target_rows_per_chunk': 500000,  # Target ~500k rows per chunk
}

# ============================================================================
# FINAL FILTERS CONFIGURATION
# ============================================================================

FINAL_FILTER_CONFIG = {
    'price_threshold': 300,    # Filter out prices above this value (% of par)
    'dip_threshold': 35,       # 2002-07 price dip threshold (% of par)
}

# ============================================================================
# YIELD DATA CONFIGURATION
# ============================================================================

# Yield data source: 'LIU_WU' (recommended) or 'FRED'
YLD_TYPE = 'LIU_WU'

# Liu-Wu zero-coupon yield curve URL (used by pull_liu_wu_yields.py, not at runtime)
LIU_WU_URL = "https://docs.google.com/spreadsheets/d/11HsxLl_u2tBNt3FyN5iXGsIKLwxvVz7t/edit?usp=sharing"

# ============================================================================
# DERIVED PATHS (DO NOT EDIT)
# ============================================================================

# Auto-detect ROOT_PATH if not set
if not ROOT_PATH or ROOT_PATH == "":
    # Derive project root from this file's location: src/stage1/_stage1_settings.py
    ROOT_PATH = Path(__file__).resolve().parent.parent.parent
else:
    # User specified ROOT_PATH, convert to Path object if needed
    ROOT_PATH = Path(ROOT_PATH)
    if str(ROOT_PATH).startswith("~"):
        ROOT_PATH = ROOT_PATH.expanduser()

STAGE0_DIR = ROOT_PATH / "_data" / "stage0"
STAGE1_DIR = ROOT_PATH / "_data" / "stage1"
STAGE1_DATA = STAGE1_DIR
LOG_DIR = ROOT_PATH / "_data" / "logs" / "stage1"
REPORT_DIR = ROOT_PATH / "_output" / "stage1_reports"

# Path to pre-pulled data directory (created by `doit pull_*` tasks)
PULLED_DIR = ROOT_PATH / "_data" / "pulled"

# Path to pre-pulled Liu-Wu yields parquet (created by `doit pull_liu_wu`)
LIU_WU_PARQUET = PULLED_DIR / "liu_wu_yields.parquet"

# ============================================================================
# AUTO-DETECT STAGE0 DATE STAMP
# ============================================================================

def get_latest_stage0_date(stage0_dir: Path, trace_member: str = "enhanced") -> str:
    """
    Auto-detect the most recent STAGE0_DATE_STAMP by finding the latest FISD parquet file.

    Searches for FISD parquet files with pattern: trace_enhanced_fisd_YYYYMMDD.parquet
    The FISD file is only present in the "enhanced" folder and serves as the date
    reference for the entire stage0 output (the main TRACE data is now written as
    hive-partitioned directories).

    Parameters
    ----------
    stage0_dir : Path
        Path to stage0 directory
    trace_member : str, default="enhanced"
        TRACE dataset type (FISD file only exists under "enhanced")

    Returns
    -------
    str
        Date stamp in YYYYMMDD format (e.g., "20251022")

    Raises
    ------
    FileNotFoundError
        If stage0 output directory or FISD parquet files don't exist
    ValueError
        If no valid date stamps can be extracted from filenames

    Examples
    --------
    >>> get_latest_stage0_date(Path("/path/to/stage0"), "enhanced")
    '20251022'
    """
    member_dir = stage0_dir / trace_member

    if not member_dir.exists():
        raise FileNotFoundError(
            f"Stage0 output directory not found: {member_dir}\n"
            f"Ensure stage0 has been run successfully and outputs exist."
        )

    # Look for FISD file: trace_{member}_fisd_YYYYMMDD.parquet
    pattern = f"trace_{trace_member}_fisd_*.parquet"
    fisd_files = list(member_dir.glob(pattern))

    if not fisd_files:
        raise FileNotFoundError(
            f"No FISD parquet files found in {member_dir}\n"
            f"Expected pattern: {pattern}\n"
            f"Ensure stage0 has been run successfully."
        )

    # Filter for valid FISD files
    # FISD file format: trace_enhanced_fisd_20251022.parquet (4 parts when split by '_')
    valid_files = []
    for f in fisd_files:
        stem = f.stem  # e.g., "trace_enhanced_fisd_20251022"
        parts = stem.split("_")

        # Should be exactly 4 parts: ["trace", "enhanced", "fisd", "20251022"]
        if len(parts) == 4 and parts[-1].isdigit() and len(parts[-1]) == 8:
            valid_files.append(f)

    if not valid_files:
        raise ValueError(
            f"No valid FISD parquet files found in {member_dir}\n"
            f"Found {len(fisd_files)} matching files, but none match expected format:\n"
            f"  Expected: trace_{trace_member}_fisd_YYYYMMDD.parquet\n"
            f"  Found files: {[f.name for f in fisd_files[:5]]}"
        )

    # Extract dates and return most recent
    dates = [f.stem.split("_")[-1] for f in valid_files]
    latest_date = max(dates)

    return latest_date


# Auto-detect STAGE0_DATE_STAMP from most recent parquet file
try:
    STAGE0_DATE_STAMP = get_latest_stage0_date(STAGE0_DIR, trace_member="enhanced")
    print(f"[AUTO-DETECT] STAGE0_DATE_STAMP = {STAGE0_DATE_STAMP} (from stage0/{TRACE_MEMBERS[0]}/ parquet files)")
except (FileNotFoundError, ValueError) as e:
    # If auto-detect fails, use a fallback or raise error
    import warnings
    warnings.warn(
        f"Could not auto-detect STAGE0_DATE_STAMP: {e}\n"
        f"Falling back to manual setting. Please check stage0 outputs.",
        UserWarning
    )
    # Fallback to manual setting (user can override)
    STAGE0_DATE_STAMP = "20251022"  # Manual fallback

# ============================================================================
# CONFIGURATION GETTER FUNCTION
# ============================================================================

def get_config() -> dict:
    """
    Returns a full configuration dictionary for Stage 1 processing.
    This centralizes all settings in one place for easy modification.
    """
    config = {
        # User settings
        "wrds_username": WRDS_USERNAME,

        # Paths
        "root_path": ROOT_PATH,
        "stage0_dir": STAGE0_DIR,
        "stage1_dir": STAGE1_DIR,
        "stage1_data": STAGE1_DATA,
        "log_dir": LOG_DIR,
        "report_dir": REPORT_DIR,

        # Input configuration
        "stage0_date_stamp": STAGE0_DATE_STAMP,
        "trace_members": TRACE_MEMBERS,

        # Execution settings
        "n_cores": N_CORES,
        "n_chunks": N_CHUNKS,

        # Filters and date range
        "date_cut_off": DATE_CUT_OFF,
        "ultra_distressed_config": ULTRA_DISTRESSED_CONFIG,
        "final_filter_config": FINAL_FILTER_CONFIG,

        # Pre-pulled data
        "pulled_dir": PULLED_DIR,

        # Yield data
        "yld_type": YLD_TYPE,
        "liu_wu_url": LIU_WU_URL,
        "liu_wu_parquet": LIU_WU_PARQUET,

        # Output settings
        "output_format": OUTPUT_FORMAT,
        # NOTE: Stage 1 always generates reports and figures (no config flags needed)
    }

    return config


def validate_config(config: dict) -> None:
    """
    Validates configuration settings and raises errors if invalid.
    """
    # Check stage0 directory exists
    if not config["stage0_dir"].exists():
        raise FileNotFoundError(
            f"Stage0 directory not found: {config['stage0_dir']}\n"
            f"Please ensure ROOT_PATH is correctly set in _stage1_settings.py"
        )

    # Check stage0 hive-partitioned data directories exist for each member
    missing_data = []
    for member in config["trace_members"]:
        member_folder = config["stage0_dir"] / member
        hive_files = list(member_folder.glob("year=*/month=*/data.parquet"))
        if not hive_files:
            missing_data.append(str(member_folder / "year=*/month=*/data.parquet"))

    if missing_data:
        raise FileNotFoundError(
            "Stage0 hive-partitioned data not found:\n" + "\n".join(missing_data) + "\n"
            "Please ensure stage0 has been run successfully."
        )

    # Check FISD file exists in the enhanced folder
    fisd_file = (
        config["stage0_dir"] / "enhanced"
        / f"trace_enhanced_fisd_{config['stage0_date_stamp']}.parquet"
    )
    if not fisd_file.exists():
        raise FileNotFoundError(
            f"Stage0 FISD file not found: {fisd_file}\n"
            "Please ensure stage0 has been run with the correct date stamp."
        )

    # Validate output format
    valid_formats = ["parquet", "csv"]
    if config["output_format"] not in valid_formats:
        raise ValueError(
            f"Invalid output_format: {config['output_format']}. "
            f"Must be one of {valid_formats}"
        )

    # Validate yield type
    valid_yld_types = ["LIU_WU", "FRED"]
    if config["yld_type"] not in valid_yld_types:
        raise ValueError(
            f"Invalid yld_type: {config['yld_type']}. "
            f"Must be one of {valid_yld_types}"
        )


def print_config_summary(config: dict) -> None:
    """
    Prints a summary of the current configuration.
    Useful for logging and debugging.
    """
    print("=" * 80)
    print("STAGE 1 CONFIGURATION SUMMARY")
    print("=" * 80)
    print(f"WRDS Username:      {config['wrds_username']}")
    print(f"Root Path:          {config['root_path']}")
    print(f"Stage0 Date Stamp:  {config['stage0_date_stamp']}")
    print(f"TRACE Members:      {', '.join(config['trace_members'])}")
    print(f"Date Cut-off:       {config['date_cut_off']}")
    print(f"Yield Source:       {config['yld_type']}")
    print(f"Output Format:      {config['output_format']}")
    print(f"CPU Cores:          {config['n_cores']}")
    print("NOTE: Stage 1 always generates comprehensive reports and figures")
    print("=" * 80)
