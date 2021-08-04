from slurmflow import Job
from slurmflow import DAG

env = {
    'BEOCAT_DATA_DIR': '/bulk/${USER}',
    'BEOCAT_SAM_DIR': '/bulk/${USER}/sam',
    'BEOCAT_BAM_DIR': '/bulk/${USER}/bam'
}

with DAG('simple_slurm_workflow', env=env) as d:
    t1 = Job('Task1', 'task1.sh', dag=d)
    t2 = Job('Task2', 'task2.sh', dag=d)
    t3 = Job('Task3', 'task3.sh', dag=d)

    [t1, t2] >> t3

    d.run()
    d.plot()
