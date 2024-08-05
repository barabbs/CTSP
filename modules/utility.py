import networkx as nx
from networkx.algorithms.connectivity import minimum_st_edge_cut
import pynauty
import json, os, time, logging
from modules import var
import numpy as np

GREEN = '\033[92m'
RED = '\033[91m'
ENDCOLOR = '\033[0m'


# def get_vector_from_variables(variables, all_variables):  # This assumes all coefficients of the variables to be 1!
#     var_set = set(variables)
#     return tuple(v in var_set for v in all_variables)
#
#
# def get_vector_from_constraint(constr, all_variables):  # This assumes all coefficients of the variables to be 1!
#     return get_vector_from_variables((v._index for v in identify_variables(constr.body)), all_variables)


def get_minimum_cut_edges(graph, n):
    best_val, best_cut = float("inf"), tuple()
    for u in graph.nodes:
        for v in graph.nodes:
            if u == v:
                continue
            new_val, new_cut = nx.minimum_cut(graph, u, v, capacity='weight')
            print(f"\t\t{new_val}  -  {new_cut}")
            if new_val < best_val and 2 <= len(new_cut[0]) <= n - 2:
                best_val, best_cut = new_val, new_cut
    return best_val, best_cut


def convert_raw_gurobi_info(raw):
    return {'sol_status': raw['Status'],
            'sol_term_cond': raw['Termination condition'],
            'time_proc': raw['Time'],
            'time_wall': float(raw['Wall time'])}


def seqence_to_str(cycles, sep=" "):
    # TODO: extend hex for i>15
    return sep.join("".join(f'{i:X}' for i in c) for c in cycles)


def bool_symb(t, file=False):
    if t is None:
        return " "
    elif t:
        return "✅" if file else GREEN + "■" + ENDCOLOR
    return "❌" if file else RED + "■" + ENDCOLOR


SEPARATOR = " "
CHARACTERS = {0.: "0", .5: "½", 1.: "1",
              (0, 0): "A", (0, 1): "a",
              (1, 0): "B", (1, 1): "b",
              (2, 0): "C", (2, 1): "c",
              None: "-"}


def prettyprint_graph_adj(n, adj, labels=None, line_start="\t"):
    if labels is None:
        labels = (list(range(n)), list(range(n)))
    # remove = False
    print(line_start + f"╲ j{SEPARATOR}" + SEPARATOR.join(str(labels[1][j]) for j in range(n)))
    print(line_start + "i ┌" + "─" * ((len(SEPARATOR) + 1) * n))
    for i in range(n):
        print(line_start + f"{labels[0][i]} │", end=SEPARATOR)
        for j in range(n):
            if i == j:
                print(" ", end=SEPARATOR)
            else:
                val = adj.get((labels[0][i], labels[1][j]), None)
                # if val == 1:
                #     remove = True
                print(CHARACTERS[val], end=SEPARATOR)
        print()
    # if remove:
    #     transl = dict()
    #     for k, v in adj.items():
    #         if v == 1:
    #             print(labels)
    #             print(k)
    #             labels[0].remove(k[0])
    #             # labels[0].remove(k[1])
    #             # labels[0][k[0]] = k[1]
    #             labels[1].remove(transl.get(k[0], k[0]))
    #             labels[1][labels[1].index(k[1])] = k[0]
    #             n -= 1
    #             break
    #             transl[k[1]] = k[0]
    #     print(labels)
    #     print(f"\n{line_start}---- without 1s ----")
    #     prettyprint_graph_adj(n, adj, labels, line_start)


#
# def save_graph_file(graph, data):
#     path = os.path.join(var.GRAPH_DATA_DIR.format(k=graph.k, n=graph.n),
#                         var.GRAPH_FILENAME.format(coding=graph._coding))
#     try:
#         with open(path, 'w') as f:
#             json.dump(data, f, indent=var.GRAPH_FILE_INDENT)
#     except FileNotFoundError:
#         os.makedirs(os.path.dirname(path), exist_ok=True)
#         with open(path, 'w') as f:
#             json.dump(data, f, indent=var.GRAPH_FILE_INDENT)


def save_run_info_file(infos, start_time, time_name, delete=False):
    filepath = var.RUN_INFO_FILEPATH.format(
        k=infos['k'], n=infos['n'], weights="-".join(str(i) for i in infos['weights']),
        strategy=infos['strategy'], generator=infos['generator'], calculators=infos['calculators'])
    try:
        if delete:
            os.remove(filepath)
            logging.info(f"Deleted run info file {os.path.basename(filepath)}")
            times = dict()
        else:
            with open(filepath, 'r') as f:
                times = json.load(f)['timings']
    except FileNotFoundError:
        times = dict()
    times[time_name] = time.time() - start_time + times.get(time_name, 0)
    infos['timings'] = times
    with open(filepath, 'w') as f:
        json.dump(infos, f, indent=var.RUN_INFO_INDENT)


def calc_chunksize(n, calc_type, tot, workers, chunktime, max_chunksize, min_chunks, **kwargs):
    params = var.EST_CALC_TIME_PARAMS[calc_type]
    calc_time = params[0] * np.exp(params[1] * n)
    est_chunksize = int(np.ceil(chunktime / calc_time))
    logging.stage(f"    [est. chunksize: {est_chunksize:<8} (est. calc_time {calc_time:.2E})")
    return min(est_chunksize, int(np.ceil(tot / (workers * min_chunks))), max_chunksize)


def set_best_gap(n, gap):
    n = str(n)
    with open(var.BEST_GAPS_FILEPATH, 'r') as f:
        gaps = json.load(f)
    gaps[n] = max(gaps.get(n, 0), gap)
    with open(var.BEST_GAPS_FILEPATH, 'w') as f:
        json.dump(gaps, f, indent=var.RUN_INFO_INDENT)


def get_best_gap(n):
    with open(var.BEST_GAPS_FILEPATH, 'r') as f:
        gaps = json.load(f)
    return gaps.get(str(n), None)
