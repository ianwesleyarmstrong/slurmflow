import subprocess
from typing import Iterable, Union
from matplotlib import pyplot as plt
import networkx as nx
from networkx.drawing.nx_agraph import graphviz_layout


_CONTEXT_MANAGER_DAG = None
plt.style.use('ggplot')


class DAG:
    """
    Some bullshit
    """
    
    __slots__ = ['graph', 'name', '_old_context_manager_dag', 'root']
    def __init__(self, name: str = 'dag') -> None:
        """ Constructor for DAG """
        self.graph = nx.DiGraph()
        self.name = name

    def __repr__(self) -> str:
        return self.name

    def __enter__(self):
        # insipired by airflow's implementation (https://github.com/apache/airflow/blob/1.10.2/airflow/models.py#L3456-L3468)
        global _CONTEXT_MANAGER_DAG
        self._old_context_manager_dag = _CONTEXT_MANAGER_DAG
        _CONTEXT_MANAGER_DAG = self
        return self

    def __exit__(self, *args, **kwargs):
        global _CONTEXT_MANAGER_DAG
        _CONTEXT_MANAGER_DAG = self._old_context_manager_dag

    def set_children(self, root: 'Node', children: Iterable) -> None:
        self._set_relationship(root, children, forward=True)

    def set_parents(self, root: 'Node', parents: Iterable) -> None:
        self._set_relationship(root, parents, forward=False)

    def _set_relationship(self, root: 'Node', rel: Union[str, Iterable], forward=True) -> None:
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
        pass


        

    def plot_DAG(self) -> None:
        plt.figure(figsize=(15, 10))
        degree = nx.degree(self.graph)
        print(degree)
        node_sizes = [v[1] * 75 for v in degree]
        nx.draw(self.graph, arrows=True, node_size=node_sizes, layout=nx.layout.spectral_layout(self.graph), node_color='cadetblue')
        plt.title(f'DAG representation of {self.name}')
        plt.show()
    



        

    
