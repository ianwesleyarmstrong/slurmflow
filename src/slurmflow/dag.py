from asyncio.events import set_event_loop
import subprocess
from typing import Iterable, Union
from matplotlib import pyplot as plt
from collections import Counter
import asyncio
import numpy as np
import networkx as nx

#from .'Job' import 'Job'


_CONTEXT_MANAGER_DAG = None
plt.style.use('ggplot')


class DAG:
    """
    Some bullshit
    """
    
    __slots__ = ['graph', 'name', '_old_context_manager_dag', '_loop']
    def __init__(self, name: str = 'dag') -> None:
        """ Constructor for DAG """
        self.graph = nx.DiGraph()
        self.name = name

    @property
    def root(self) -> 'Job':
        return [n for n,d in self.graph.in_degree() if d==0][0]


    def __repr__(self) -> str:
        return f'{self.name}'

    def __enter__(self):
        # insipired by airflow's implementation (https://github.com/apache/airflow/blob/1.10.2/airflow/models.py#L3456-L3468)
        global _CONTEXT_MANAGER_DAG
        self._old_context_manager_dag = _CONTEXT_MANAGER_DAG
        _CONTEXT_MANAGER_DAG = self
        return self

    def __exit__(self, *args, **kwargs):
        global _CONTEXT_MANAGER_DAG
        _CONTEXT_MANAGER_DAG = self._old_context_manager_dag

    def set_children(self, root: 'Job', children: Iterable) -> None:
        self._set_relationship(root, children, forward=True)

    def set_parents(self, root: 'Job', parents: Iterable) -> None:
        self._set_relationship(root, parents, forward=False)

    def _set_relationship(self, root: 'Job', rel: Union[str, Iterable], forward=True) -> None:
        try:
            rel_list = list(rel)
        except TypeError:
            rel_list = [rel]
        
        for node in rel_list:
            if forward:
                pair = (root, node)
            else:
                pair = (node, root)
            self.graph.add_edge(*pair)

    def submit_DAG(self) -> None:
        # find way to extend environment variables in DAG
        for node in self.graph.nodes():
            # job has upstream dependencies
            if upstream := node.upstream_jobs:
                upstream_ids = []
                for job in upstream:
                    if not job.id:
                        upstream_job_id = job.submit()
                        print(node.name, upstream_job_id)
                        upstream_ids.append(upstream_job_id)
                    else:
                        upstream_ids.append(job.id)
                # dependencies have been resolved
                node.submit(upstream_ids)
            else:
                # no job dependencies
                node.submit()







    def _create_dag_positions(self):
        starting_node = self.root
        levels = nx.single_source_dijkstra_path_length(self.graph, starting_node)
        
        #print(levels)
        level_counts = Counter(levels.values())
        level_decrement = level_counts.copy()
        pos_mapping = dict()
        for k, v in levels.items():
            count = level_counts[v]
            if count == 1:
                pos_mapping[k] = (v, 0)
                #print(k, v, 0)
            else:
                bound = level_counts[v] // 2
                y_min, y_max = -bound, bound
                arr = np.linspace(y_min, y_max, level_counts[v])
                level_decrement[v] -= 1
                cur = level_decrement[v]
                scaled_val = arr[cur]
                pos_mapping[k] = (v, scaled_val)
                #print(k, v, scaled_val)
        return pos_mapping

    def plot_DAG(self) -> None:
        plt.figure(figsize=(15, 10))
        degree = nx.degree(self.graph)
        #print(degree)
        pos = self._create_dag_positions()
        node_size=[len(v.name) * 400 for v in self.graph.nodes()]
        nx.draw_networkx(self.graph, pos=pos,node_size=node_size, \
             arrows=True, node_color='cadetblue')
        plt.title(f'DAG representation of {self.name}')
        ax = plt.gca()
        ax.axes.xaxis.set_visible(False)
        ax.axes.yaxis.set_visible(False)
        plt.grid(True)
        plt.show()
    



        

    
