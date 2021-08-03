import unittest
from slurmflow.dag import DAG
from slurmflow.job import Job


class TestDAG(unittest.TestCase):

    def test_name(self):
        dag = DAG('test')
        self.assertEqual(dag.name, 'test')

    def test_source(self):
        dag = DAG()
        job1 = Job('j1', 'nothing.sh', dag=dag)
        job2 = Job('j2', 'nothing.sh', dag=dag)
        job1 >> job2
        self.assertEqual(job1, dag.source)


if __name__ == '__main__':
    unittest.main()
