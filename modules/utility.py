from pyomo.core.expr import identify_variables
import networkx as nx
import pynauty
import json, os, time
from modules import var

GREEN = '\033[92m'
RED = '\033[91m'
ENDCOLOR = '\033[0m'


def get_vector_from_variables(variables, all_variables):  # This assumes all coefficients of the variables to be 1!
    var_set = set(variables)
    return tuple(v in var_set for v in all_variables)


def get_vector_from_constraint(constr, all_variables):  # This assumes all coefficients of the variables to be 1!
    return get_vector_from_variables((v._index for v in identify_variables(constr.body)), all_variables)


def get_graph_from_code(code):
    graph = nx.MultiDiGraph()
    for i, cover in enumerate(code):
        for k, cycle in cover:
            # TODO: use nx.add_path(G, [0, 1, 2]) instead?
            graph.add_edges_from((a, cycle[(b + 1) % k], i) for b, a in enumerate(cycle))
    return graph


def get_nauty_graph(n, k, graph):
    adj = dict()
    colors = list(set() for i in range(k - 1))
    for u, neigh in graph.adjacency():
        new = list()
        for v, edges in neigh.items():
            k = len(edges)
            if k == 1:
                new.append(v)
            else:
                new.append(n)
                adj[n] = [v, ]
                colors[k - 2].add(n)
                n += 1
        adj[u] = new
    # return adj, colors
    return pynauty.Graph(n, directed=True, adjacency_dict=adj, vertex_coloring=colors)


def get_adjacency_matrix(n, k, graph, weights):
    adj = dict(sum((tuple(((i, j), 0) for j in range(n) if j != i) for i in range(n)), start=tuple()))
    for u, neigh in graph.adjacency():
        for v, edges in neigh.items():
            adj[(u, v)] = len(edges) / k  # TODO: Implement weights
    return adj


def seqence_to_str(cycles, sep=" "):
    # TODO: extend hex for i>15
    return sep.join("".join(f'{i:X}' for i in c) for c in cycles)


def bool_symb(t, file=False):
    if t is None:
        return " "
    elif t:
        return "✅" if file else GREEN + "■" + ENDCOLOR
    return "❌" if file else RED + "■" + ENDCOLOR


def save_graph_file(graph, data):
    path = os.path.join(var.GRAPH_DATA_DIR.format(k=graph.k, n=graph.n), var.GRAPH_FILENAME.format(coding=graph._coding))
    try:
        with open(path, 'w') as f:
            json.dump(data, f, indent=var.GRAPH_FILE_INDENT)
    except FileNotFoundError:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w') as f:
            json.dump(data, f, indent=var.GRAPH_FILE_INDENT)

def timing(function, *args, **kwargs):
    start = time.process_time_ns()
    result = function(*args, **kwargs)
    return time.process_time_ns() - start, result
