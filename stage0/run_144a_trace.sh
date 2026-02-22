#!/bin/bash
#$ -S /bin/bash             # Ensure the job uses Bash shell
#$ -V                       # Export environment variables (important for Python envs)
#$ -cwd                     # Run from current working directory
#$ -o stage0/logs/03_144a.out    # Log standard output
#$ -e stage0/logs/03_144a.err    # Log standard error

python3 ./src/stage0/_run_144a_trace.py