import subprocess
from .dag import DAG
from typing import List

_CONTEXT_MANAGER_DAG = None


class Job:

    def __init__(self, name: str, script: str, dag: DAG = None) -> None:
        self.name = name
        # print(self.name)
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
        return f'{self.__class__.__name__}({self.name}, {self.script}, \
        {self._dag})'

    def submit(self, upstream_ids: List[str] = None) -> str:
        command = ['sbatch', f'--job-name={self.name}']
        if upstream_ids:
            slurm_dependency = '--dependency=afterok:' + ':'.join(upstream_ids)
            command.append(slurm_dependency)
        if self._dag.env:
            env_flag = '--export='
            env_vars = [f'{k}={v}' for k, v in self._dag.env.items()]
            env_export = env_flag + ','.join(env_vars)
            command.append(env_export)
        # script has to come last in order for environment variables to pass
        command.append(self.script)
        command_str = ' '.join(command)
        print(f'Sbatch command: {command_str}')
        p = subprocess.Popen(command, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        out, err = p.communicate()
        out, err = out.decode('utf-8').strip(), err.decode('utf-8').strip()
        if out and not err:
            job_id = out.split(' ')[-1]
            self.id = job_id
            return job_id
        else:
            return err
