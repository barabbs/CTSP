from modules.coding import Coding, Cover
from modules import graphics
from modules import var
import pynauty
import numpy as np
import networkx as nx


class Graph(object):
    LAZY_ATTRS = {'graph', 'vector', 'digraph', 'weighted_digraph', 'nauty_graph', 'adjacency_matrix'}

    def __init__(self, n, k, weights, coding, **kwargs):
        self.n, self.k, self.weights = n, k, weights
        self.coding = Coding(tuple(Cover(cycles=c, weight=w) for c, w in zip(coding, self.weights)))
        # self.graph, self.nauty_graph, self.adjacency_matrix = None, None, None

    # PRIVATE UTILITIES

    def _get_graph(self):
        graph = nx.MultiDiGraph()
        for i, cover in enumerate(self.coding):
            for k, cycle in cover:
                # TODO: use nx.add_path(G, [0, 1, 2]) instead?
                graph.add_edges_from((a, cycle[(b + 1) % k], i) for b, a in enumerate(cycle))
        return graph

    def _edge_pairs_generator(self):
        yield from ((u, v) for u in self.graph.nodes for v in self.graph.nodes if u != v)

    def edge_count_generator(self, weight=False):  # TODO: Implement weights
        if weight:
            yield from ((u, v, self.graph.number_of_edges(u, v) / self.k) for u, v in self._edge_pairs_generator())
        else:
            yield from ((u, v, len(edges)) for u, neigh in self.graph.adjacency() for v, edges in neigh.items())

    def _get_vector(self):
        return np.fromiter((self.graph.number_of_edges(u, v) for u, v in self._edge_pairs_generator()),
                           dtype=np.float32) / self.k

    def _get_digraph(self):
        digraph = nx.DiGraph(self.graph)
        for u, v, c in self.edge_count_generator():
            for i in range(c - 1):
                new = (u, v, i)
                digraph.add_node(new)
                digraph.add_edges_from(((u, new), (new, v)))
        return digraph

    def _get_weighted_digraph(self):
        digraph = nx.DiGraph()
        digraph.add_weighted_edges_from(self.edge_count_generator())
        return digraph

    def _get_nauty_graph(self):
        adj = dict((u, list()) for u in self.graph.nodes)
        n = self.n
        colors = list(set() for _ in range(self.k - 1))
        for u, v, k in self.edge_count_generator():
            if k == 1:
                adj[u].append(v)
            else:
                adj[u].append(n)
                adj[n] = [v, ]
                colors[k - 2].add(n)
                n += 1
        return pynauty.Graph(n, directed=True, adjacency_dict=adj, vertex_coloring=colors)

    def _get_adjacency_matrix(self):
        return dict(((u, v), w) for u, v, w in self.edge_count_generator(weight=True))

    def __getattr__(self, item):
        assert item in Graph.LAZY_ATTRS
        setattr(self, item, getattr(self, f"_get_{item}")())
        return getattr(self, item)

    # GRAPHICS and FILES

    def draw(self, properties=None, gap=None):
        graphics.plot_graph(self.graph, self.coding, properties, gap)
        graphics.save_graph_drawing(n=self.n, k=self.k, coding=self.coding)

    def show(self, properties=None, gap=None):
        graphics.plot_graph(self.graph, self.coding, properties, gap)
        graphics.show_graph_drawing()

    def __repr__(self):
        return f"Graph {self.coding}"
