import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
from modules import var
import os

plt.rcParams["font.family"] = "monospace"
COLORS = ("#aa1835", "#f79f0e", "#005b96")
TEXT_COLORS = {True: "xkcd:green",
               False: "xkcd:red",
               None: "xkcd:gray",
               }

BASE_WIDTH, STEP_WIDTH = 2, 0

EDGE_DASH, EDGE_STYLE = 10, 'arc3,rad={rad}'
EDGE_CURVATURE = 0.2 / 3
MULT_CURVATURE = 0.1 / 3


def edge_style(u, v, i, edges, pos):
    curv = min(sum(1 for (_u, _v, _i) in edges if _u == v and _v == u), EDGE_CURVATURE)
    mult = tuple(filter(lambda k: k[0] == u and k[1] == v, edges))
    curv += MULT_CURVATURE * (mult.index((u, v, i)) - (len(mult) - 1) / 2)
    dist = np.linalg.norm(pos[u] - pos[v])
    curv = curv / dist
    return {'width': BASE_WIDTH + (len(mult) - 1 - mult.index((u, v, i))) * STEP_WIDTH,
            # 'style': (mult.index((u, v, i)) * EDGE_DASH, (EDGE_DASH, EDGE_DASH * (len(mult) - 1))),
            'edge_color': COLORS[i],
            'connectionstyle': EDGE_STYLE.format(rad=curv),
            'alpha': 1}


def plot_graph(graph, coding, properties=None):
    properties = properties or dict()
    plt.figure(figsize=(4, 4))
    try:
        pos = nx.planar_layout(graph)
    except nx.NetworkXException:
        pos = nx.shell_layout(graph)
    nx.draw_networkx_nodes(graph, pos=pos, node_color='black', edgecolors='black')
    nx.draw_networkx_labels(graph, pos=pos, font_color='white')
    # print(self.edges)
    for edge in graph.edges:
        nx.draw_networkx_edges(graph, pos=pos, edgelist=(edge,), **edge_style(*edge, graph.edges, pos))

    # for i, c in enumerate(self._apply_translation(labels) if labels is not None else self.original_code):
    for i, c in enumerate(coding):
        plt.annotate(str(c), (0.01, 0.95 - 0.05 * i), xycoords='axes fraction')
    for i, item in enumerate(properties.items()):
        name, val = item
        plt.annotate(f"{name.upper()}", (0.99, 0.95 - 0.05 * i), xycoords='axes fraction', c=TEXT_COLORS[val],
                     horizontalalignment='right')
    plt.axis('off')
    plt.tight_layout()


def save_graph_drawing(n, k, coding):
    path = os.path.join(var.GRAPH_DRAW_DIR.format(k=k, n=n), var.DRAWINGS_FILENAME.format(coding=coding))
    try:
        plt.savefig(fname=path, dpi=var.DRAWINGS_DPI)
    except FileNotFoundError:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        plt.savefig(fname=path, dpi=var.DRAWINGS_DPI)
        plt.clf()
        plt.close('all')


def show_graph_drawing():
    plt.show()
