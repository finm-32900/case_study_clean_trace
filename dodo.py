"""Run or update the project. This file uses the `doit` Python package. It works
like a Makefile, but is Python-based.

"""

#######################################
## Configuration and Helpers for PyDoit
#######################################
## Make sure the src folder is in the path
import sys

sys.path.insert(1, "./src/")

from pathlib import Path

from settings import config

DOIT_CONFIG = {"backend": "sqlite3", "dep_file": "./.doit-db.sqlite"}

DATA_DIR = config("DATA_DIR")
OUTPUT_DIR = config("OUTPUT_DIR")


##################################
## Begin rest of PyDoit tasks here
##################################


def task_config():
    """Create empty directories for data and output if they don't exist"""
    return {
        "actions": ["ipython ./src/settings.py"],
        "targets": [DATA_DIR, OUTPUT_DIR],
        "file_dep": ["./src/settings.py"],
        "clean": [],
    }


def task_pull_fisd():
    """Pull FISD reference data from WRDS"""
    return {
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/pull_fisd.py",
        ],
        "targets": [
            DATA_DIR / "pulled" / "fisd_issue.parquet",
            DATA_DIR / "pulled" / "fisd_issuer.parquet",
        ],
        "file_dep": [
            "./src/settings.py",
            "./src/wrds_utils.py",
            "./src/pull_fisd.py",
        ],
        "clean": [],
        "verbosity": 2,
    }


def task_pull_trace():
    """Pull raw TRACE data from WRDS (Enhanced, Standard, 144A)"""

    # Each subtask targets its hive-partitioned directory under DATA_DIR.
    # Example file: _data/trace_enhanced/year=2024/month=01/data.parquet

    yield {
        "name": "trace_enhanced",
        "doc": "Pull raw trace_enhanced from WRDS",
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/pull_trace_enhanced.py",
        ],
        "targets": [DATA_DIR / "pulled" / "trace_enhanced"],
        "file_dep": [
            "./src/settings.py",
            "./src/wrds_utils.py",
            "./src/pull_utils.py",
            "./src/pull_trace_enhanced.py",
        ],
        "clean": [],
        "verbosity": 2,
    }

    yield {
        "name": "trace_standard",
        "doc": "Pull raw trace_standard from WRDS",
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/pull_trace_standard.py",
        ],
        "targets": [DATA_DIR / "pulled" / "trace_standard"],
        "file_dep": [
            "./src/settings.py",
            "./src/wrds_utils.py",
            "./src/pull_utils.py",
            "./src/pull_trace_standard.py",
        ],
        "clean": [],
        "verbosity": 2,
    }

    yield {
        "name": "trace_144a",
        "doc": "Pull raw trace_144a from WRDS",
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/pull_trace_144a.py",
        ],
        "targets": [DATA_DIR / "pulled" / "trace_144a"],
        "file_dep": [
            "./src/settings.py",
            "./src/wrds_utils.py",
            "./src/pull_utils.py",
            "./src/pull_trace_144a.py",
        ],
        "clean": [],
        "verbosity": 2,
    }


def task_build_fisd_universe():
    """Build FISD bond universe from pulled reference data"""
    return {
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/build_fisd_universe.py",
        ],
        "targets": [
            DATA_DIR / "fisd_universe.parquet",
            DATA_DIR / "fisd_universe_offamt.parquet",
        ],
        "file_dep": [
            "./src/settings.py",
            "./src/build_fisd_universe.py",
            DATA_DIR / "pulled" / "fisd_issue.parquet",
            DATA_DIR / "pulled" / "fisd_issuer.parquet",
        ],
        "clean": [],
        "verbosity": 2,
    }


def task_filter_trace_fisd():
    """Filter raw TRACE data to FISD universe CUSIPs"""

    for dataset in ["trace_enhanced", "trace_standard", "trace_144a"]:
        yield {
            "name": dataset,
            "doc": f"Filter {dataset} to FISD universe CUSIPs",
            "actions": [
                "ipython ./src/settings.py",
                f"ipython ./src/filter_trace_fisd.py -- --DATASET={dataset}",
            ],
            "file_dep": [
                "./src/settings.py",
                "./src/clean_utils.py",
                "./src/filter_trace_fisd.py",
                DATA_DIR / "fisd_universe.parquet",
            ],
            "clean": [],
            "verbosity": 2,
        }


def task_run_stage0():
    """Run Stage 0 TRACE cleaning (Dick-Nielsen) for each data type"""

    members = ["enhanced", "standard", "144a"]
    for member in members:
        yield {
            "name": member,
            "doc": f"Run Stage 0 cleaning for {member} TRACE",
            "actions": [
                f"cd stage0 && python _run_{member}_trace.py",
            ],
            "task_dep": ["filter_trace_fisd"],
            "file_dep": [
                f"./stage0/_run_{member}_trace.py",
                "./stage0/_trace_settings.py",
                "./stage0/clean_trace_local.py",
                "./config.py",
            ],
            "targets": [f"./stage0/{member}/"],
            "clean": [],
            "verbosity": 2,
        }


def task_run_stage1():
    """Run Stage 1 analytics pipeline (duration, convexity, credit spreads)"""
    return {
        "actions": [
            "mkdir -p stage1/data stage1/logs",
            "cd stage1 && python _run_stage1.py",
        ],
        "task_dep": ["run_stage0"],
        "file_dep": [
            "./stage1/_run_stage1.py",
            "./stage1/_stage1_settings.py",
            "./stage1/create_daily_stage1.py",
            "./stage1/stage1_pipeline.py",
            "./stage1/helper_functions.py",
            "./config.py",
        ],
        "targets": ["./stage1/data/"],
        "clean": [],
        "verbosity": 2,
    }


def task_pull_open_source_bond():
    """Pull open-source bond data from OpenBondAssetPricing.com"""
    return {
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/pull_open_source_bond.py",
        ],
        "targets": [
            DATA_DIR / "pulled" / "corporate_bond_returns.parquet",
            DATA_DIR / "pulled" / "treasury_bond_returns.parquet",
        ],
        "file_dep": [
            "./src/settings.py",
            "./src/pull_open_source_bond.py",
        ],
        "clean": [],
        "verbosity": 2,
    }


def task_test_stage1_vs_open_source():
    """Run pytest comparison of Stage 1 output vs open-source OSBAP data"""
    return {
        "actions": [
            f"python -m pytest ./src/test_stage1_vs_open_source.py -v "
            f"--tb=short --junitxml={OUTPUT_DIR / 'test_results.xml'}",
        ],
        "task_dep": ["run_stage1"],
        "targets": [
            OUTPUT_DIR / "test_results.xml",
        ],
        "file_dep": [
            "./src/settings.py",
            "./src/test_stage1_vs_open_source.py",
            DATA_DIR / "pulled" / "corporate_bond_returns.parquet",
        ],
        "clean": [],
        "verbosity": 2,
    }
