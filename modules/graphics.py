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
GAP_COLOR = "xkcd:black"

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


def plot_graph(graph, coding, properties=None, gap=None):
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
    if gap is None:
        plt.annotate(f"---", (0.99, 0.95), xycoords='axes fraction', c=TEXT_COLORS[None],
                     horizontalalignment='right')
    else:
        plt.annotate(f"{gap:.3f}", (0.99, 0.95), xycoords='axes fraction', c=GAP_COLOR,
                     horizontalalignment='right')
    for i, item in enumerate(properties.items()):
        name, val = item
        plt.annotate(f"{name.upper()}", (0.99, 0.90 - 0.05 * i), xycoords='axes fraction', c=TEXT_COLORS[val],
                     horizontalalignment='right')
    plt.axis('off')
    plt.tight_layout()


def save_graph_drawing(n, k, coding):
    path = os.path.join(var.DRAWINGS_DIR.format(k=k, n=n), var.DRAWINGS_FILENAME.format(coding=coding))
    try:
        plt.savefig(fname=path, dpi=var.DRAWINGS_DPI)
    except FileNotFoundError:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        plt.savefig(fname=path, dpi=var.DRAWINGS_DPI)
        plt.clf()
        plt.close('all')


def show_graph_drawing():
    plt.show()


####    LATEX    ####

DEFAULT_LATEX_OPTIONS = {"default_edge_options": "baseedgestyle",
                         # "default_node_options": "nodestyle",
                         "tikz_options": "graphstyle",
                         }
LATEX_EDGE_STYLES = ["edgestyle0", "edgestyle1"]
LATEX_DOUBLEEDGE_STYLE = "doubleedge"
LATEX_DIGON_STYLE = "digonstyle"
LATEX_NODES_STYLE = "nodestyle"
LATEX_CODING_STYLE = "codingstyle"


def _get_latex_edges_styles(graph, colors=None):
    styles = dict()
    colors = colors or tuple(range(graph.k))
    for u, v in set(graph.graph.edges()):
        ks = tuple(graph.graph[u][v].keys())
        if len(ks) == 2:
            styles[(u, v)] = f"{LATEX_DOUBLEEDGE_STYLE}=" + ":".join(LATEX_EDGE_STYLES[colors[ks[k]]] for k in ks)
        else:
            styles[(u, v)] = LATEX_EDGE_STYLES[colors[ks[0]]]
        if (v, u) in graph.graph.edges():
            styles[(u, v)] += f", {LATEX_DIGON_STYLE}"
    return styles


def _get_coding_text(coding):
    return "{" + "\\\\".join("$\\big[\\," +
                             "\\,\\big|\\,".join("\\,".join(str(p) for p in cycle) for cycle in cover.cycles) +
                             "\\,\\big]$" for cover in coding.covers) + "}"


def create_latex(graph=None, filename=None, coding=None, coding_pos=None, pos='pos', colors=None, begin=None, end=None,
                 insertfile=None, insert=None, pass_by=None, nx_graph=None, **new_options):
    coding = coding or getattr(graph, "coding", None)
    options = DEFAULT_LATEX_OPTIONS.copy()
    if nx_graph is None:
        options["edge_options"] = _get_latex_edges_styles(graph, colors)
        for edge, opt in new_options.pop("edge_options", dict()).items():
            options["edge_options"][edge] += f", {opt}"
    options.update(new_options)
    nx_graph = nx_graph or graph.weighted_digraph
    if pos is None:
        try:
            pos = nx.rescale_layout_dict(nx.planar_layout(nx_graph), scale=3)
        except nx.NetworkXException:
            pos = nx.shell_layout(nx_graph)
        print(dict((k, tuple(round(i, 2) for i in v)) for k, v in pos.items()))
    latex = nx.drawing.to_latex_raw(nx_graph, pos=pos, **options)
    for (u, v), points in (pass_by or dict()).items():
        if not type(points[0]) == tuple:
            points = (points,)
        latex = latex.replace(f"({u}) to ({v})", f"({u}) to {' to '.join(str(p) for p in points)} to ({v})")
    lines = latex.split("\n")
    if coding_pos is not None:
        coding_text = f"      \\node[{LATEX_CODING_STYLE}] at {coding_pos} {_get_coding_text(coding)};\n"
    else:
        coding_text = ""
    if insertfile is not None:
        with open(os.path.join(var.LATEX_GRAPH_DIR, insertfile), "r") as f:
            insert = f.read()
    else:
        insert = insert or ""
    latex = f"""{begin or lines[0]}\n{coding_text}{f"      \\draw[{LATEX_NODES_STYLE}]"}\n{'\n'.join(lines[2:-2])}\n{insert}{end or '\n'.join(lines[-2:])}"""

    with open(os.path.join(var.LATEX_GRAPH_DIR, filename), "w") as f:
        f.write(f"%{coding}\n\n{latex}")
