#!/bin/bash
#
#SBATCH --job-name=slurmflow_test_2
#SBATCH --output=slurmflow_test_2.out
#
#SBATCH --ntasks=1
#SBATCH --time=10:00
#SBATCH --mem-per-cpu=1G

cat data2.txt > data3.txt

env

echo "task 2 complete"