# Slurmflow

  > Slurmflow is a job dependency manager built to interact with slurm clusters! It serves as an abstraction between your data processing and the implementation details of Slurm. Slurmflow represents your tasks as a Directed Acyclic graph, where each node is a sbatch script to be called upon. When you "run" the dag, Slurflow recursively traverses the graph from sink (no outgoing nodes) to the source (no incoming nodes) and submits each script to slurm using the --dependency flag.
  
### Example
  > Here is a simple example with three scripts (t1, t2, t3), with t3 requiring outputs of the previous two tasks. When creating each Job, all that's needed is a name for the node and the filename of a sbatch script to call. Once your jobs are created, it's possible to use the bitshift operators [<<, >>] to set dependencies. The operator 1 >> 2 denotes that 1 must finish before 2 can run, while 1 << 2 denotes the opposite. If your pipeline allows for certain steps to be run in parallel, you can add them into a list [] and perform the bitshift operations. For a more conventional syntax, you can alseo use the set_upstream or set_downstream methods like so t1.set_downstream(t2). The equivalent notation using bitshifting is t1 >> t2. 
  
#### `Workflow.py`
```python 
from slurmflow import Job
from slurmflow import DAG

with DAG('simple_slurm_workflow', env=env) as d:
    t1 = Job('Task1', 'task1.sh', dag=d)
    t2 = Job('Task2', 'task2.sh', dag=d)
    t3 = Job('Task3', 'task3.sh', dag=d)


[t1, t2] >> t3

d.run()
d.plot()
```
### Scripts
#### `task1.sh`
```bash
#!/bin/bash
#SBATCH --job-name=slurmflow_test_1
#SBATCH --output=slurmflow_test_1.out
#SBATCH --ntasks=1
#SBATCH --time=10:00
#SBATCH --mem-per-cpu=1G

cat data1.txt > data4.txt

env

echo "task 1 complete"
```
#### `task2.sh`
```bash
#!/bin/bash
#SBATCH --job-name=slurmflow_test_2
#SBATCH --output=slurmflow_test_2.out
#SBATCH --ntasks=1
#SBATCH --time=10:00
#SBATCH --mem-per-cpu=1G

cat data2.txt > data3.txt

env

echo "task 2 complete"
```

#### `task3.sh`
```bash
#!/bin/bash
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
```

### Data Files
#### `data1.txt`
```
1
2
3
4
5
```

#### `data2.txt`
```
10
11
12
13
14
15
```
#### `final.txt`
```
10
11
12
13
14
15
1
2
3
4
5
```

