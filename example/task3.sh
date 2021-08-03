#!/bin/bash
#
#SBATCH --job-name=slurmflow_test_3
#SBATCH --output=slurmflow_test_3.out
#SBATCH --ntasks=1
#SBATCH --time=10:00
#SBATCH --mem-per-cpu=1G

cat data3.txt >> final.txt
cat data4.txt >> final.txt
rm data3.txt data4.txt

env

echo "temporary files removed"