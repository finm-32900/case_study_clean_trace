"""Run or update the project. This file uses the `doit` Python package. It works
like a Makefile, but is Python-based.

"""

#######################################
## Configuration and Helpers for PyDoit
#######################################
## Make sure the src folder is in the path
import sys
from datetime import date as _date
from os import environ
from pathlib import Path

sys.path.insert(1, "./src/")

from settings import config, get_start_date, get_end_date

SRC_PY_FILES = sorted(Path("./src").rglob("*.py"))

DOIT_CONFIG = {"backend": "sqlite3", "dep_file": "./.doit-db.sqlite"}

DATA_DIR = config("DATA_DIR")
OUTPUT_DIR = config("OUTPUT_DIR")
OS_TYPE = config("OS_TYPE")

## Helpers for handling Jupyter Notebook tasks
environ["PYDEVD_DISABLE_FILE_VALIDATION"] = "1"


def _month_range(start, end):
    """Return (year, month) tuples from start to end inclusive."""
    if start is None or end is None:
        return []
    result = []
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        result.append((y, m))
        m += 1
        if m > 12:
            y, m = y + 1, 1
    return result


def _hive_targets(base, name, months):
    """Explicit parquet targets; falls back to directory if no date range."""
    if not months:
        return [base / name]
    return [
        base / name / f"year={y}" / f"month={m:02d}" / "data.parquet"
        for y, m in months
    ]


_MONTHS = _month_range(get_start_date(), get_end_date())

# fmt: off
def jupyter_execute_notebook(notebook_path):
    return f"jupyter nbconvert --execute --to notebook --ClearMetadataPreprocessor.enabled=True --inplace {notebook_path}"
def jupyter_to_html(notebook_path, output_dir=OUTPUT_DIR):
    return f"jupyter nbconvert --to html --output-dir={output_dir} {notebook_path}"
def jupyter_clear_output(notebook_path):
    return f"jupyter nbconvert --ClearOutputPreprocessor.enabled=True --ClearMetadataPreprocessor.enabled=True --inplace {notebook_path}"
# fmt: on


def mv(from_path, to_path):
    """Move a file to a folder"""
    from_path = Path(from_path)
    to_path = Path(to_path)
    to_path.mkdir(parents=True, exist_ok=True)
    if OS_TYPE == "nix":
        command = f"mv {from_path} {to_path}"
    else:
        command = f"move {from_path} {to_path}"
    return command


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


def task_pull():
    """Pull all raw datasets from external sources"""

    yield {
        "name": "fama_french",
        "doc": "Pull Fama-French SIC code classification files from Dartmouth",
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/pull_fama_french_sic.py",
        ],
        "targets": [
            DATA_DIR / "pulled" / "ff17_sic_ranges.parquet",
            DATA_DIR / "pulled" / "ff30_sic_ranges.parquet",
        ],
        "file_dep": [
            "./src/settings.py",
            "./src/pull_fama_french_sic.py",
        ],
        "clean": [],
        "verbosity": 2,
    }

    yield {
        "name": "fisd",
        "doc": "Pull FISD reference data from WRDS",
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/pull_fisd.py",
        ],
        "targets": [
            DATA_DIR / "pulled" / "fisd_issue.parquet",
            DATA_DIR / "pulled" / "fisd_issuer.parquet",
            DATA_DIR / "pulled" / "fisd_amt_out_hist.parquet",
            DATA_DIR / "pulled" / "fisd_ratings_sp.parquet",
            DATA_DIR / "pulled" / "fisd_ratings_moodys.parquet",
            DATA_DIR / "pulled" / "fisd_redemption.parquet",
        ],
        "file_dep": [
            "./src/settings.py",
            "./src/wrds_utils.py",
            "./src/pull_fisd.py",
        ],
        "clean": [],
        "verbosity": 2,
    }

    yield {
        "name": "liu_wu",
        "doc": "Pull Liu-Wu zero-coupon Treasury yields from Google Sheets",
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/pull_liu_wu_yields.py",
        ],
        "targets": [
            DATA_DIR / "pulled" / "liu_wu_yields.parquet",
        ],
        "file_dep": [
            "./src/settings.py",
            "./src/pull_liu_wu_yields.py",
        ],
        "clean": [],
        "verbosity": 2,
    }

    yield {
        "name": "osbap",
        "doc": "Pull open-source bond data from OpenBondAssetPricing.com",
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/pull_open_source_bond.py",
        ],
        "targets": [
            DATA_DIR / "pulled" / "corporate_bond_returns.parquet",
            DATA_DIR / "pulled" / "treasury_bond_returns.parquet",
            DATA_DIR / "pulled" / "osbap_linker.parquet",
        ],
        "file_dep": [
            "./src/settings.py",
            "./src/pull_open_source_bond.py",
        ],
        "clean": [],
        "verbosity": 2,
    }

    for trace_name in ["trace_144a", "trace_enhanced", "trace_standard"]:
        yield {
            "name": trace_name,
            "doc": f"Pull raw {trace_name} from WRDS",
            "actions": [
                "ipython ./src/settings.py",
                f"ipython ./src/pull_{trace_name}.py",
            ],
            "targets": _hive_targets(DATA_DIR / "pulled", trace_name, _MONTHS),
            "file_dep": [
                "./src/settings.py",
                "./src/wrds_utils.py",
                "./src/pull_utils.py",
                f"./src/pull_{trace_name}.py",
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
            "targets": _hive_targets(DATA_DIR, f"{dataset}_fisd", _MONTHS),
            "task_dep": [f"pull:{dataset}"],
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
                f"python ./src/stage0/_run_{member}_trace.py",
            ],
            "task_dep": [f"filter_trace_fisd:trace_{member}"],
            "file_dep": [
                f"./src/stage0/_run_{member}_trace.py",
                "./src/stage0/_trace_settings.py",
                "./src/stage0/clean_trace_local.py",
                "./src/config.py",
                DATA_DIR / "fisd_universe.parquet",
                DATA_DIR / "fisd_universe_offamt.parquet",
            ],
            "targets": _hive_targets(DATA_DIR / "stage0", member, _MONTHS),
            "clean": [],
            "verbosity": 2,
        }


def task_run_stage1():
    """Run Stage 1 analytics pipeline (duration, convexity, credit spreads)"""
    return {
        "actions": [
            "mkdir -p _data/stage1 _data/logs/stage1 _output/stage1_reports",
            "python ./src/stage1/_run_stage1.py",
        ],
        "task_dep": ["run_stage0"],
        "file_dep": [
            "./src/stage1/_run_stage1.py",
            "./src/stage1/_stage1_settings.py",
            "./src/stage1/create_daily_stage1.py",
            "./src/stage1/stage1_pipeline.py",
            "./src/stage1/helper_functions.py",
            "./src/config.py",
            # Pre-pulled data consumed by Stage 1
            DATA_DIR / "pulled" / "liu_wu_yields.parquet",
            DATA_DIR / "pulled" / "fisd_issue.parquet",
            DATA_DIR / "pulled" / "fisd_amt_out_hist.parquet",
            DATA_DIR / "pulled" / "fisd_ratings_sp.parquet",
            DATA_DIR / "pulled" / "fisd_ratings_moodys.parquet",
            DATA_DIR / "pulled" / "fisd_redemption.parquet",
            DATA_DIR / "pulled" / "osbap_linker.parquet",
            DATA_DIR / "pulled" / "ff17_sic_ranges.parquet",
            DATA_DIR / "pulled" / "ff30_sic_ranges.parquet",
        ],
        "targets": [
            DATA_DIR / "stage1" / f"stage1_{_date.today().strftime('%Y%m%d')}.parquet",
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
            *SRC_PY_FILES,
            DATA_DIR / "pulled" / "corporate_bond_returns.parquet",
        ],
        "clean": [],
        "verbosity": 2,
    }


##################################
## Notebook tasks
##################################

notebook_tasks = {
    "01_data_sources_overview_ipynb": {
        "path": "./src/01_data_sources_overview_ipynb.py",
        "file_dep": [
            DATA_DIR / "pulled" / "corporate_bond_returns.parquet",
            DATA_DIR / "pulled" / "ff17_sic_ranges.parquet",
            DATA_DIR / "pulled" / "ff30_sic_ranges.parquet",
            DATA_DIR / "pulled" / "fisd_amt_out_hist.parquet",
            DATA_DIR / "pulled" / "fisd_issue.parquet",
            DATA_DIR / "pulled" / "fisd_issuer.parquet",
            DATA_DIR / "pulled" / "fisd_ratings_moodys.parquet",
            DATA_DIR / "pulled" / "fisd_ratings_sp.parquet",
            DATA_DIR / "pulled" / "fisd_redemption.parquet",
            DATA_DIR / "pulled" / "liu_wu_yields.parquet",
            DATA_DIR / "pulled" / "osbap_linker.parquet",
            DATA_DIR / "pulled" / "treasury_bond_returns.parquet",
        ],
        "task_dep": [
            "pull:trace_144a",
            "pull:trace_enhanced",
            "pull:trace_standard",
        ],
        "targets": [],
    },
}


# fmt: off
def task_run_notebooks():
    """Preps the notebooks for presentation format.
    Execute notebooks if the script version of it has been changed.
    """
    for notebook in notebook_tasks.keys():
        pyfile_path = Path(notebook_tasks[notebook]["path"])
        notebook_path = pyfile_path.with_suffix(".ipynb")
        yield {
            "name": notebook,
            "actions": [
                f"jupytext --to notebook --output {notebook_path} {pyfile_path}",
                jupyter_execute_notebook(notebook_path),
                jupyter_to_html(notebook_path),
                mv(notebook_path, OUTPUT_DIR),
            ],
            "file_dep": [
                pyfile_path,
                *notebook_tasks[notebook]["file_dep"],
            ],
            "task_dep": notebook_tasks[notebook].get("task_dep", []),
            "targets": [
                OUTPUT_DIR / f"{notebook}.html",
                *notebook_tasks[notebook]["targets"],
            ],
            "clean": True,
            "verbosity": 2,
        }
# fmt: on


sphinx_targets = [
    "./docs/index.html",
]


def task_build_chartbook_site():
    """Compile Sphinx Docs"""
    notebook_scripts = [
        Path(notebook_tasks[notebook]["path"])
        for notebook in notebook_tasks.keys()
    ]
    file_dep = [
        "./README.md",
        "./chartbook.toml",
        *notebook_scripts,
    ]

    return {
        "actions": [
            "chartbook build -f",
        ],
        "targets": sphinx_targets,
        "file_dep": file_dep,
        "task_dep": [
            "run_notebooks",
        ],
        "clean": True,
    }
