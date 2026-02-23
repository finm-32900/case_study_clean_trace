# Clean TRACE: Corporate Bond Transaction Data Pipeline

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

This repository is a restructured fork of the [TRACE Data Pipeline](https://github.com/Alexander-M-Dickerson/trace-data-pipeline) by Alexander Dickerson, Cesare Robotti, and Giulio Rossetti, part of the [Open Bond Asset Pricing](https://openbondassetpricing.com/) project. It has been reorganized by **Jeremiah Bejarano** to use [PyDoit](https://pydoit.org/) for task orchestration, Polars-based hive partitioning for incremental data processing, and a clean `_data/` + `_output/` directory layout.

All of the core cleaning logic, filters, and bond analytics originate from Dickerson et al. This fork restructures the execution framework while preserving the underlying methodology.

---

## Why This Pipeline Matters

TRACE (Trade Reporting and Compliance Engine) is the primary source of U.S. corporate bond transaction data, but the raw data contains numerous errors: decimal-shifted prices, duplicate reports, cancelled and corrected trades, and erroneous spikes. Producing research-quality bond panels requires systematic cleaning.

This pipeline implements the Dick-Nielsen (2009, 2014) cancellation, correction, and reversal filters, the van Binsbergen, Nozawa & Schwert (2025) filters, a decimal-shift corrector, a bounce-back filter, and agency trade de-duplication, among other steps. It then enriches the cleaned data with bond analytics (duration, convexity, yields, credit spreads) via QuantLib, credit ratings, FISD bond characteristics, and Fama-French industry classifications. The result is a clean daily corporate bond panel ready for asset pricing research.

For full details on the methodology, see Dickerson, Robotti & Rossetti (2025), "[Common Pitfalls in the Evaluation of Corporate Bond Strategies](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4575879)."

---

## Quick Start

### 1. Clone the repository

```bash
git clone <this-repo-url>
cd case_study_clean_trace
```

### 2. Set up your environment file

```bash
cp .env.example .env
```

Edit `.env` and fill in your WRDS credentials:

```
WRDS_USERNAME="your_wrds_username"
```

**Important:** By default, the pipeline processes only a small 2-month sample (January--February 2024) so you can run end-to-end quickly without pulling 20+ years of data. To process the full TRACE history, uncomment and set explicit dates:

```
START_DATE=2002-07-01
END_DATE=2025-12-31
```

If `START_DATE` is earlier than a dataset's earliest available date, it is automatically clamped (e.g. TRACE Enhanced starts 2002-07-01, Standard starts 2024-10-01).

### 3. Create a conda environment and install dependencies

```bash
conda create -n clean_trace python=3.11 -y
conda activate clean_trace
pip install -r requirements.txt
```

### 4. Ensure WRDS `.pgpass` is configured

If you haven't already set up passwordless WRDS authentication:

```bash
python -c "import wrds; db = wrds.Connection(); db.close()"
```

This will prompt for your WRDS username and password and create the `.pgpass` file.

### 5. Run the pipeline

```bash
doit                    # Run the full pipeline
doit list               # See all available tasks
doit run_stage0         # Run only Stage 0 (data pulls + cleaning)
doit run_stage1         # Run only Stage 1 (bond analytics)
```

PyDoit handles all task dependencies, runs stages in the correct order, and skips tasks whose outputs are already up to date.

---

## Running on RCC Midway3 (UChicago)

Worker nodes on Midway3 do not have internet access, so data pulls (WRDS, Fama-French, etc.) cannot run inside a batch job. Instead, the instructor pulls data once on the head node, and students rsync it into their own project space before running the pipeline.

### Instructor workflow (one-time setup)

The instructor runs the data pull interactively on the Midway3 head node (which has internet access):

```bash
cd /project/finm32900/<instructor_username>/case_study_clean_trace
module load python/anaconda-2024.10
source activate /project/finm32900/<instructor_username>/envs/clean_trace
doit pull
```

This downloads all raw data to `_data/pulled/`. Students will rsync from this location.

### Student workflow

#### 1. Clone the repository into project space

```bash
cd /project/finm32900/${USER}
git clone <this-repo-url> case_study_clean_trace
cd case_study_clean_trace
```

Use `/project/` space (not `/home/`) to avoid inode quotas and take advantage of the larger storage allocation.

#### 2. Set up your environment file

```bash
cp .env.example .env
```

Edit `.env` and set your WRDS username and the desired date range:

```
WRDS_USERNAME="your_wrds_username"
START_DATE=2002-07-01
END_DATE=2025-12-31
```

#### 3. Create conda environment

Do **not** pip install into the base Anaconda module -- the system-managed packages (pandas, numpy, etc.) will conflict with the versions this pipeline requires. Create an isolated conda environment in project space:

```bash
# Load conda (never run `conda init` on RCC -- use `source activate`)
module load python/anaconda-2024.10

# Create environment in project space (avoids home directory inode quota)
conda create --prefix=/project/finm32900/${USER}/envs/clean_trace python=3.11 -y

# Activate and install
source activate /project/finm32900/${USER}/envs/clean_trace
pip install -r requirements.txt
```

To reactivate in future sessions:

```bash
module load python/anaconda-2024.10
source activate /project/finm32900/${USER}/envs/clean_trace
```

#### 4. Configure WRDS `.pgpass`

If you haven't already set up passwordless WRDS authentication:

```bash
python -c "import wrds; db = wrds.Connection(); db.close()"
```

This will prompt for your WRDS username and password and create the `~/.pgpass` file. (This step requires the head node, which has network access to WRDS.)

#### 5. Submit the batch job

```bash
sbatch run-pipeline.sbatch
```

The sbatch script automatically:
1. Syncs pre-pulled data from the instructor's project directory to your `_data/pulled/`
2. Marks all `pull` tasks as ignored (so doit does not attempt network downloads)
3. Runs the full pipeline (build FISD universe, filter TRACE, Stage 0 cleaning, Stage 1 analytics)

#### Monitoring your job

```bash
squeue -u ${USER}                                # Check job status
tail -f <jobid>_clean-trace-pipeline.out         # Follow stdout
tail -f <jobid>_clean-trace-pipeline.err         # Follow stderr
```

The job requests 16 CPUs with 128 GB total memory and an 8-hour time limit. Polars and Stage 1 automatically use all available cores.

---

## Pipeline Overview

The pipeline is orchestrated by `dodo.py` and proceeds in stages:

**Data Pulls** — Downloads raw data from WRDS (TRACE Enhanced, Standard, 144A, FISD, credit ratings, Liu-Wu yields, Fama-French industries, OSBAP linker). Data is stored in hive-partitioned Parquet files under `_data/pulled/`.

**FISD Universe** — Builds a reference universe of bonds from FISD and filters TRACE transactions to bonds in that universe.

**Stage 0: Cleaning** — Applies the full suite of Dick-Nielsen and related filters to produce clean daily bond panels. Each month is processed independently to manage memory. Output goes to `_data/stage0/`.

**Stage 1: Analytics** — Enriches the cleaned data with QuantLib bond analytics (duration, convexity, YTM, credit spreads), credit ratings, FISD characteristics, and Fama-French industry classifications. Output goes to `_data/stage1/`.

All intermediate and derived data lives under `_data/` (reconstructible, not version-controlled). Reports and notebooks go to `_output/`.

---

## Repository Structure

```
case_study_clean_trace/
├── dodo.py                  # PyDoit task definitions (the build system)
├── run-pipeline.sbatch      # Slurm batch script for RCC Midway3
├── chartbook.toml           # Documentation site configuration
├── requirements.txt         # Python dependencies
├── .env.example             # Example environment variables
├── LICENSE                  # MIT License (Alexander Dickerson)
│
├── src/                     # All source code
│   ├── config.py            # Shared pipeline configuration
│   ├── settings.py          # Environment/CLI config loader
│   ├── pull_*.py            # Data pull scripts (7 sources)
│   ├── build_fisd_universe.py
│   ├── filter_trace_fisd.py
│   ├── stage0/              # Stage 0: Dick-Nielsen cleaning
│   └── stage1/              # Stage 1: Bond analytics
│
├── docs_src/                # Documentation source (markdown)
│
├── _data/                   # All data (generated, not version-controlled)
│   ├── pulled/              # Raw data from WRDS
│   ├── stage0/              # Cleaned daily panels
│   └── stage1/              # Enriched analytics
│
└── _output/                 # Reports, notebooks, test results
```

---

## Configuration

| File | Purpose |
|------|---------|
| `.env` | WRDS username, `START_DATE`/`END_DATE` (defaults to 2-month sample), `N_CORES` |
| `src/config.py` | Output format, TRACE dataset members, figure toggles |
| `src/settings.py` | Reads `.env` via `python-decouple`, resolves directory paths |
| `src/stage0/_trace_settings.py` | Stage 0 filter switches and parameters |
| `src/stage1/_stage1_settings.py` | Stage 1 analytics configuration |

---

## Citation & Credits

### Original Authors

This pipeline is built on the work of **Alexander Dickerson**, **Cesare Robotti**, and **Giulio Rossetti**. If you use this pipeline in your research, please cite:

```bibtex
@unpublished{dickerson2025pitfalls,
  author = {Dickerson, Alexander and Robotti, Cesare and Rossetti, Giulio},
  title  = {Common pitfalls in the evaluation of corporate bond strategies},
  year   = {2025},
  note   = {Working Paper}
}

@unpublished{dickerson2025constructing,
  author = {Dickerson, Alexander and Rossetti, Giulio},
  title  = {Constructing TRACE Corporate Bond Datasets},
  year   = {2025},
  note   = {Working Paper}
}
```

### Links

- **Original repository**: [Alexander-M-Dickerson/trace-data-pipeline](https://github.com/Alexander-M-Dickerson/trace-data-pipeline)
- **Open Bond Asset Pricing**: [openbondassetpricing.com](https://openbondassetpricing.com/)
- **PyBondLab** (companion for factor construction): [GiulioRossetti94/PyBondLab](https://github.com/GiulioRossetti94/PyBondLab)

### This Fork

Restructured by **Jeremiah Bejarano**. The reorganization introduces PyDoit orchestration, hive-partitioned data storage, incremental processing, and a simplified execution workflow, while preserving the original cleaning methodology and analytics.

---

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details. Original copyright (c) 2025 Alexander Dickerson.
