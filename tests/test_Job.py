import unittest
from slurmflow.dag import DAG
from slurmflow.job import Job


class TestDAG(unittest.TestCase):

    def generate_dag(self):
        dag = DAG()
        job1 = Job('j1', 'nothing.sh', dag=dag)
        job2 = Job('j2', 'nothing.sh', dag=dag)
        job3 = Job('j3', 'nothing.sh', dag=dag)
        job4 = Job('j4', 'nothing.sh', dag=dag)
        job5 = Job('j5', 'nothing.sh', dag=dag)
        job6 = Job('j6', 'nothing.sh', dag=dag)
        job7 = Job('j7', 'nothing.sh', dag=dag)
        job1 >> [job2, job3] >> job4 >> [job5, job6] >> job7
        # add None as first index to allow for easier indexing
        jobs = [None, job1, job2, job3, job4, job5, job6, job7]
        return jobs, dag

    def test_name(self):
        dag = DAG('test')
        self.assertEqual(dag.name, 'test')

    def test_set_downstream(self):
        dag = DAG()
        job1 = Job('j1', 'nothing.sh', dag=dag)
        job2 = Job('j2', 'nothing.sh', dag=dag)
        job1.set_downstream(job2)
        dummy_downstream_set = set()
        dummy_downstream_set.add(job2)
        self.assertEqual(job1.downstream_jobs, dummy_downstream_set)

    def test_set_upstream(self):
        dag = DAG()
        job1 = Job('j1', 'nothing.sh', dag=dag)
        job2 = Job('j2', 'nothing.sh', dag=dag)
        job2.set_upstream(job1)
        dummy_upstream_set = set()
        dummy_upstream_set.add(job1)
        self.assertEqual(dummy_upstream_set, job2.upstream_jobs)

    def test_lshift(self):
        dag = DAG()
        job1 = Job('j1', 'nothing.sh', dag=dag)
        job2 = Job('j2', 'nothing.sh', dag=dag)
        job2 << job1
        dummy_upstream_set = set()
        dummy_upstream_set.add(job1)
        self.assertEqual(dummy_upstream_set, job2.upstream_jobs)

    def test_rshift(self):
        dag = DAG()
        job1 = Job('j1', 'nothing.sh', dag=dag)
        job2 = Job('j2', 'nothing.sh', dag=dag)
        job1 >> job2
        dummy_downstream_set = set()
        dummy_downstream_set.add(job2)
        self.assertEqual(job1.downstream_jobs, dummy_downstream_set)


if __name__ == '__main__':
    unittest.main()
