#!/bin/bash
#
#SBATCH --job-name=slurmflow_test_1
#SBATCH --output=slurmflow_test_1.out
#
#SBATCH --ntasks=1
#SBATCH --time=10:00
#SBATCH --mem-per-cpu=1G

cat data1.txt > data4.txt

env

echo "task 1 complete"