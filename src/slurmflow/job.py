import subprocess
from .dag import DAG, _CONTEXT_MANAGER_DAG
from collections import defaultdict
from typing import Iterable, List

_CONTEXT_MANAGER_DAG = None

class Job():

    def __init__(self, name: str, script: str, dag: DAG = None) -> None:
        self.name = name
        self.script = script
        self.id = None
        if not dag and _CONTEXT_MANAGER_DAG:
            dag = _CONTEXT_MANAGER_DAG
        if dag:
            self._dag = dag

    @property
    def downstream_jobs(self):
        return set(self._dag.graph.successors(self))

    @property
    def upstream_jobs(self):
        return set(self._dag.graph.predecessors(self))


    def set_downstream(self, downstream) -> None:
        self._dag.set_children(self, downstream)

    def set_upstream(self, upstream) -> None:
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
        return f'{self.name}'
    # def __repr__(self) -> str:
    #     return f' Name: {self.name}, Script: {self.script}, ID: {self.id}, Upstream: {self.upstream_jobs}, Downstream: {self.downstream_jobs}'

    def submit(self, upstream_ids: List[str] = None) -> str:
        command = ['sbatch', f'--job-name={self.name}', self.script]
        if upstream_ids:
            slurm_dependency = '--dependency=afterok:' + ':'.join(upstream_ids)
            command.append(slurm_dependency)
        p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        out, err = out.decode('utf-8').strip(), err.decode('utf-8').strip()
        if out and not err:
            job_id = out[-1] 
            self.id = job_id
            return job_id 
        else:
            return err

