import unittest
from slurmflow.dag import DAG


class DAGTest(unittest.TestCase):

    def test_name(self):
        dag = DAG('test')
        self.assertEqual(dag.name, 'test')
