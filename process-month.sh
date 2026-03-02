#!/bin/bash
# process-month.sh -- Process a single (stage, dataset, year, month) task.
# Called by GNU Parallel via srun in run-pipeline-parallel.sbatch.
#
# Usage:
#   bash process-month.sh <stage> <dataset> <year> <month> [work_dir]
#
# Arguments:
#   stage    : "filter" or "stage0"
#   dataset  : For filter: trace_enhanced, trace_standard, trace_144a
#              For stage0: enhanced, standard, 144a
#   year     : 4-digit year (e.g. 2024)
#   month    : 1-12
#   work_dir : Optional working directory (defaults to current directory)

set -euo pipefail

STAGE="$1"
DATASET="$2"
YEAR="$3"
MONTH="$4"
WORK_DIR="${5:-$(pwd)}"

cd "${WORK_DIR}"

echo "[$(date)] process-month.sh: stage=${STAGE} dataset=${DATASET} year=${YEAR} month=${MONTH}"

if [ "$STAGE" = "filter" ]; then
    # ---------------------------------------------------------------
    # FILTER: Read one raw TRACE partition, filter to FISD CUSIPs,
    # write one filtered partition. Uses library functions directly
    # because filter_trace_fisd.py does not support date-range args.
    # ---------------------------------------------------------------
    python -c "
import sys, logging
sys.path.insert(0, './src')
logging.basicConfig(level=logging.INFO,
    format='%(asctime)s %(levelname)-8s %(name)s: %(message)s')

from pathlib import Path
from settings import config
import polars as pl
from clean_utils import read_partition, write_clean_partition

DATA_DIR = Path(config('DATA_DIR'))
PULL_DIR = DATA_DIR / 'pulled'

dataset = '${DATASET}'
year = ${YEAR}
month = ${MONTH}

# Skip if output already exists
output_dataset = f'{dataset}_fisd'
out_path = DATA_DIR / output_dataset / f'year={year}' / f'month={month:02d}' / 'data.parquet'
if out_path.exists():
    print(f'[{dataset}] {year}-{month:02d}: output already exists, skipping')
    sys.exit(0)

# Skip if no input data
in_path = PULL_DIR / dataset / f'year={year}' / f'month={month:02d}' / 'data.parquet'
if not in_path.exists():
    print(f'[{dataset}] {year}-{month:02d}: no input partition, skipping')
    sys.exit(0)

# Load FISD universe CUSIPs
fisd_universe = pl.read_parquet(DATA_DIR / 'fisd_universe.parquet')
fisd_cusips = fisd_universe['complete_cusip'].to_list()

# Read, filter, write
df = read_partition(PULL_DIR, dataset, year, month)
n_before = len(df)
df = df.filter(pl.col('cusip_id').is_in(fisd_cusips))
n_after = len(df)
print(f'[{dataset}] {year}-{month:02d}: {n_before} -> {n_after} rows (-{n_before - n_after})')
write_clean_partition(df, DATA_DIR, output_dataset, year, month)
"

elif [ "$STAGE" = "stage0" ]; then
    # ---------------------------------------------------------------
    # STAGE0: Dick-Nielsen cleaning for one month. Uses START_DATE /
    # END_DATE env vars to restrict _iter_partitions to this month.
    # The existing skip-if-exists logic in clean_trace_local.py
    # provides additional idempotency.
    # ---------------------------------------------------------------

    # Compute first and last day of the target month
    DATE_RANGE=$(python -c "
from calendar import monthrange
y, m = ${YEAR}, ${MONTH}
last_day = monthrange(y, m)[1]
print(f'{y}-{m:02d}-01')
print(f'{y}-{m:02d}-{last_day:02d}')
")
    export START_DATE=$(echo "$DATE_RANGE" | head -1)
    export END_DATE=$(echo "$DATE_RANGE" | tail -1)

    MEMBER="${DATASET}"
    echo "[$(date)] stage0 ${MEMBER} START_DATE=${START_DATE} END_DATE=${END_DATE}"

    python "./src/stage0/_run_${MEMBER}_trace.py"

else
    echo "ERROR: Unknown stage '${STAGE}'. Expected 'filter' or 'stage0'."
    exit 1
fi

echo "[$(date)] process-month.sh: DONE stage=${STAGE} dataset=${DATASET} year=${YEAR} month=${MONTH}"
