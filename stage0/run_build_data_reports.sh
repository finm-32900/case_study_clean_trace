#!/bin/bash
#$ -S /bin/bash                 # Ensure the job uses Bash shell
#$ -V                           # Export environment variables (important for Python envs)
#$ -cwd                         # Run from current working directory
#$ -pe onenode 1
#$ -l m_mem_free=32G
#$ -o stage0/logs/_data_reports.out    # Log standard output
#$ -e stage0/logs/_data_reports.err    # Log standard error

python3 ./src/stage0/_build_error_files.py