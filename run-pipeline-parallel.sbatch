#!/bin/bash
#SBATCH --job-name=clean-trace-parallel
#SBATCH --output=%j_clean-trace-parallel.out
#SBATCH --error=%j_clean-trace-parallel.err
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=8
#SBATCH --cpus-per-task=2
#SBATCH --mem-per-cpu=8000
#SBATCH --time=8:00:00
#SBATCH --account=finm32900

# ---------------------------------------------------------------
# Parallel TRACE Cleaning Pipeline — Slurm batch script for RCC Midway3
#
# Parallelizes filter_trace_fisd (CUSIP filtering) and stage0
# (Dick-Nielsen cleaning) across (dataset, month) combinations
# using GNU Parallel + srun. Keeps build_fisd_universe and stage1
# sequential as required by their cross-month dependencies.
#
# Resource defaults: 4 nodes × 8 tasks = 32 parallel workers,
# each with 2 CPUs and 16 GB RAM. Adjust --nodes to scale up.
#
# Prerequisites: same as run-pipeline.sbatch (conda env, .env, etc.)
#
# Submit with:
#   sbatch run-pipeline-parallel.sbatch
#
# Checkpoint/resume: resubmit the same script to retry failed jobs.
# GNU Parallel's --resume flag skips jobs that completed successfully.
#
# Clean restart: delete _data/logs/parallel/*_joblog.txt, then resubmit.
# ---------------------------------------------------------------

set -euo pipefail

# --- Configuration ---
PROJECT_BASE="/project/finm32900"
WORK_DIR="${PROJECT_BASE}/${USER}/case_study_clean_trace"
INSTRUCTOR_DATA="${PROJECT_BASE}/case_study_clean_trace/_data/pulled"
STUDENT_DATA="${WORK_DIR}/_data/pulled"
LOG_DIR="${WORK_DIR}/_data/logs/parallel"

cd "${WORK_DIR}"
echo "[$(date)] Working directory: ${WORK_DIR}"
echo "[$(date)] SLURM_JOB_ID: ${SLURM_JOB_ID}"
echo "[$(date)] SLURM_NTASKS: ${SLURM_NTASKS}"

# --- Environment setup ---
module load python/anaconda-2024.10
module load parallel
source activate "${PROJECT_BASE}/${USER}/envs/clean_trace"
echo "[$(date)] Activated conda environment"

pip install --quiet -r requirements.txt
echo "[$(date)] Dependencies up to date"

# GNU Parallel recommendation: raise user process limit
ulimit -u 10000

# ===============================================================
# SEQUENTIAL PHASE 1: Data sync + build_fisd_universe
# ===============================================================
echo "[$(date)] === SEQUENTIAL PHASE 1: Setup ==="

# Rsync pulled data from instructor's directory
echo "[$(date)] Syncing pulled data from instructor directory..."
mkdir -p "${STUDENT_DATA}"
rsync -a --info=progress2 "${INSTRUCTOR_DATA}/" "${STUDENT_DATA}/"
echo "[$(date)] Data sync complete"

# Mark pull tasks as done (data already synced)
echo "[$(date)] Marking pull tasks as ignored..."
doit pull
doit ignore build_chartbook_site
echo "[$(date)] Pull tasks ignored"

# Build FISD universe (single task, fast)
echo "[$(date)] Running build_fisd_universe..."
doit build_fisd_universe
echo "[$(date)] build_fisd_universe complete"

# ===============================================================
# GENERATE JOB LISTS
# ===============================================================
echo "[$(date)] Generating job lists..."
mkdir -p "${LOG_DIR}"

# Generate job lists by scanning actual input partitions on disk.
# This avoids creating jobs for months where no data was pulled.
python -c "
import sys
sys.path.insert(0, './src')
from pathlib import Path
from settings import config
from pull_utils import existing_partitions

DATA_DIR = Path(config('DATA_DIR'))
PULL_DIR = DATA_DIR / 'pulled'

# --- Filter jobs: scan pulled partitions ---
filter_jobs = []
for dataset in ['trace_enhanced', 'trace_standard', 'trace_144a']:
    parts = existing_partitions(PULL_DIR, dataset)
    for y, m in sorted(parts):
        filter_jobs.append(f'filter {dataset} {y} {m}')

with open('${LOG_DIR}/filter_jobs.txt', 'w') as f:
    f.write('\n'.join(filter_jobs) + '\n' if filter_jobs else '')

# --- Stage0 jobs: scan pulled partitions ---
# (pre-generated from pulled partitions since filter is 1:1 with pulled)
stage0_jobs = []
member_map = {
    'trace_enhanced': 'enhanced',
    'trace_standard': 'standard',
    'trace_144a': '144a',
}
for dataset, member in member_map.items():
    parts = existing_partitions(PULL_DIR, dataset)
    for y, m in sorted(parts):
        stage0_jobs.append(f'stage0 {member} {y} {m}')

with open('${LOG_DIR}/stage0_jobs.txt', 'w') as f:
    f.write('\n'.join(stage0_jobs) + '\n' if stage0_jobs else '')

print(f'Generated {len(filter_jobs)} filter jobs, {len(stage0_jobs)} stage0 jobs')
"
echo "[$(date)] Job lists written to ${LOG_DIR}/"

N_FILTER=$(wc -l < "${LOG_DIR}/filter_jobs.txt" | tr -d ' ')
N_STAGE0=$(wc -l < "${LOG_DIR}/stage0_jobs.txt" | tr -d ' ')
echo "[$(date)] Filter jobs: ${N_FILTER}, Stage0 jobs: ${N_STAGE0}"

# ===============================================================
# PARALLEL PHASE 1: filter_trace_fisd
# ===============================================================
echo "[$(date)] === PARALLEL PHASE 1: filter_trace_fisd ==="

SRUN_CMD="srun --exclusive -N1 -n1"

if [ "${N_FILTER}" -gt 0 ]; then
    parallel --delay .2 -j "${SLURM_NTASKS}" --colsep ' ' \
        --joblog "${LOG_DIR}/filter_joblog.txt" --resume \
        "${SRUN_CMD} bash ${WORK_DIR}/process-month.sh {1} {2} {3} {4} ${WORK_DIR}" \
        :::: "${LOG_DIR}/filter_jobs.txt"
    echo "[$(date)] Filter phase complete"
else
    echo "[$(date)] No filter jobs to run"
fi

# ===============================================================
# PARALLEL PHASE 2: stage0 (Dick-Nielsen cleaning)
# ===============================================================
echo "[$(date)] === PARALLEL PHASE 2: stage0 ==="

if [ "${N_STAGE0}" -gt 0 ]; then
    parallel --delay .2 -j "${SLURM_NTASKS}" --colsep ' ' \
        --joblog "${LOG_DIR}/stage0_joblog.txt" --resume \
        "${SRUN_CMD} bash ${WORK_DIR}/process-month.sh {1} {2} {3} {4} ${WORK_DIR}" \
        :::: "${LOG_DIR}/stage0_jobs.txt"
    echo "[$(date)] Stage0 phase complete"
else
    echo "[$(date)] No stage0 jobs to run"
fi

# ===============================================================
# SEQUENTIAL PHASE 2: stage1
# ===============================================================
echo "[$(date)] === SEQUENTIAL PHASE 2: stage1 ==="
mkdir -p _data/stage1 _data/logs/stage1 _output/stage1_reports
python ./src/stage1/_run_stage1.py
echo "[$(date)] Stage1 complete"

echo "[$(date)] === PIPELINE COMPLETE ==="
