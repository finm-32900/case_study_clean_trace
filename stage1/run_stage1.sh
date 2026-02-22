#!/bin/bash
#$ -S /bin/bash             # Ensure the job uses Bash shell
#$ -V                       # Export environment variables (important for Python envs)
#$ -cwd                     # Run from current working directory
#$ -l m_mem_free=24G        # Request 24GB memory
#$ -o stage1/logs/stage1.out       # Log standard output
#$ -e stage1/logs/stage1.err       # Log standard error

python3 ./src/stage1/_run_stage1.py
