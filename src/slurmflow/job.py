import subprocess
from .dag import DAG, _CONTEXT_MANAGER_DAG
from collections import defaultdict
from typing import Iterable

_CONTEXT_MANAGER_DAG = None

class Job():

    def __init__(self, name: str, script: str, dag: DAG = None) -> None:
        self.name = name
        self.script = script
        self.upstream_jobs = set()
        self.downstream_jobs = set()
        self.id = None
        if not dag and _CONTEXT_MANAGER_DAG:
            dag = _CONTEXT_MANAGER_DAG
        if dag:
            self._dag = dag

    def _sanatize_tasks(self, tasks) -> set():
        if type(tasks) == list:
            san = set(tasks)
        else:
            san = set()
            san.add(tasks)
        return san



    def set_downstream(self, downstream) -> None:
        ds = self._sanatize_tasks(downstream)
        self.downstream_jobs.update(ds)
        self._dag.set_children(self, downstream)

    def set_upstream(self, upstream) -> None:
        us = self._sanatize_tasks(upstream)
        self.upstream_jobs.update(us)
        self._dag.set_parents(self, upstream)

    def __lshift__(self, other) -> None:
        self.set_upstream(other)
        return other

    def __rshift__(self, other) -> None:
        self.set_downstream(other)
        return other
    
    def __rrshift__(self, other) -> None:
        self.__lshift__(other)
        return self

    def __rlshift__(self, other) -> None:
        self.__rshift__(other)
        return self

    def __str__(self) -> str:
        return f'{self.name}'

    def __repr__(self) -> str:
        return f' Name: {self.name}, Script: {self.script}, ID: {self.id}, Upstream: {self.upstream_jobs}, Downstream: {self.downstream_jobs}'

    def submit(self) -> str:
        command = ['sbatch', f'--job-name={self.name}', self.job_script]
        p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate().decode('utf-8').strip()
        if out and not err:
            job_id = out[-1] 
            self.id = job_id
            return job_id 
        else:
            return err

