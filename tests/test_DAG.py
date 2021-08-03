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

    def test_source_property(self):
        jobs, dag = self.generate_dag()
        # source can potentially return a list, so we grab the first element
        self.assertEqual(jobs[1], dag.source[0])

    def test_sink_property(self):
        jobs, dag = self.generate_dag()
        # source can potentially return a list, so we grab the first element
        self.assertEqual(jobs[-1], dag.sink[0])


if __name__ == '__main__':
    unittest.main()
