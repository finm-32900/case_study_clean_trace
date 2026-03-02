#!/bin/bash
#SBATCH --job-name=clean-trace-pipeline
#SBATCH --output=%j_clean-trace-pipeline.out
#SBATCH --error=%j_clean-trace-pipeline.err
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=16
#SBATCH --mem-per-cpu=8000
#SBATCH --time=8:00:00
#SBATCH --account=finm32900

# ---------------------------------------------------------------
# Clean TRACE Pipeline — Slurm batch script for RCC Midway3
#
# Prerequisites (done ONCE by the instructor on the head node):
#   cd /project/finm32900/<instructor>/case_study_clean_trace
#   doit pull
#
# Prerequisites (done ONCE by each student on the head node):
#   1. Clone repo into /project/finm32900/${USER}/case_study_clean_trace
#   2. Create conda env (see README)
#   3. Configure .env and WRDS .pgpass (see README)
#
# Submit with:
#   sbatch run-pipeline.sbatch
# ---------------------------------------------------------------

set -euo pipefail

# --- Configuration ---
PROJECT_BASE="/project/finm32900"
WORK_DIR="${PROJECT_BASE}/${USER}/case_study_clean_trace"
INSTRUCTOR_DATA="${PROJECT_BASE}/case_study_clean_trace/_data/pulled"
STUDENT_DATA="${WORK_DIR}/_data/pulled"

# --- Change to working directory ---
cd "${WORK_DIR}"
echo "[$(date)] Working directory: ${WORK_DIR}"

# --- Activate conda environment ---
module load python/anaconda-2024.10
source activate "${PROJECT_BASE}/${USER}/envs/clean_trace"
echo "[$(date)] Activated conda environment"

# --- Install/update dependencies ---
# Safety net in case requirements.txt changed. If a truly new package is
# needed, install it on the head node first (worker nodes lack internet).
pip install --quiet -r requirements.txt
echo "[$(date)] Dependencies up to date"

# --- Rsync pulled data from instructor's project directory ---
echo "[$(date)] Syncing pulled data from instructor directory..."
mkdir -p "${STUDENT_DATA}"
rsync -a --info=progress2 "${INSTRUCTOR_DATA}/" "${STUDENT_DATA}/"
echo "[$(date)] Data sync complete"

# --- Tell doit to skip all pull tasks (data already synced) ---
echo "[$(date)] Marking pull tasks as ignored..."
doit ignore pull
doit ignore build_chartbook_site
echo "[$(date)] Pull tasks ignored"

# --- Run the full pipeline ---
echo "[$(date)] Starting pipeline..."
doit
echo "[$(date)] Pipeline complete"
