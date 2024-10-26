from itertools import permutations, combinations
from functools import cache
from modules import var

import json
import logging


@cache
def partition(n, maximum=float("inf"), minimum=2):
    return (((n,),) if n <= maximum else tuple()) + sum(
        (tuple((i, *tail) for tail in partition(n - i, i)) for i in range(min(n - minimum, maximum), minimum - 1, -1)),
        start=tuple())


def partitions_generator(n, k=2):
    if not k == 2:
        raise NotImplemented
    partitions = tuple(partition(n, minimum=2))
    for i_p, p_1 in enumerate(partitions):
        for p_2 in partitions[i_p:]:
            if p_2 == (n,):
                continue
            yield p_1, p_2
    partition.cache_clear()


def manual_partitions_generator(n, k=2):
    if not k == 2:
        raise NotImplemented
    with open(var.GENERATOR_SETTINGS_FILEPATH.format(n=n, k=k), 'r') as f:
        data = json.load(f)
    parts = data['parts']
    for p_1, p_2 in parts:
        yield tuple(p_1), tuple(p_2)


@cache
def cycles(nodes, parts, minimum=None):
    if len(parts) == 0:
        return (tuple(),)
    min_check = minimum is not None and parts[0] == minimum[0]
    result = tuple()
    for head in combinations(nodes, parts[0]):
        top = head[0]
        if min_check and top < minimum[1]:
            continue
        perms = tuple(permutations(head[1:]))
        reminder = nodes.difference(head)
        result += sum(
            (tuple(((top, *chunk), *tail) for chunk in perms) for tail in
             cycles(reminder, parts[1:], minimum=(parts[0], top))),
            start=tuple())
    return result


@cache
def get_non_integer(head, succ, part):
    n_heads = tuple()
    perms = tuple(permutations(head[1:]))
    for chunk in perms:
        n_head = (head[0], *chunk)
        if all(n_head[(i + 1) % part] != succ[v] for i, v in enumerate(n_head)):
            # print(f"\t\t\t{n_head} - YES")
            n_heads += (n_head,)
        # else:
        #     print(f"\t\t\t{n_head} - NO")
    return n_heads


@cache
def cycles_no_integer(nodes, parts, succ, minimum=None):
    if len(parts) == 0:
        return (tuple(),)
    min_check = minimum is not None and parts[0] == minimum[0]
    result = tuple()
    for head in combinations(nodes, parts[0]):
        top = head[0]
        if min_check and top < minimum[1]:
            continue
        reminder = nodes.difference(head)
        result += sum(
            (tuple((n_head, *tail) for n_head in get_non_integer(head, succ, parts[0])) for tail in
             cycles_no_integer(reminder, parts[1:], succ=succ, minimum=(parts[0], top))),
            start=tuple())
    return result


def codings_generator(n, parts, gen_type, k=2):
    if not k == 2:
        raise NotImplemented
    nodes = tuple(range(n))
    p_1, p_2 = parts
    c_1 = tuple(nodes[sum(p_1[:i]):sum(p_1[:i + 1])] for i, p in enumerate(p_1))
    if gen_type == "full":
        for c_2 in cycles(frozenset(nodes), p_2):
            yield c_1, c_2
        partition.cache_clear()
        cycles.cache_clear()
    elif gen_type == "half":
        succ = tuple(nodes[sum(p_1[:i]) + (j + 1) % p] for i, p in enumerate(p_1) for j in range(p))
        for c_2 in cycles_no_integer(frozenset(nodes), p_2, succ=succ):
            yield c_1, c_2
        cycles_no_integer.cache_clear()
        get_non_integer.cache_clear()
    else:
        raise NotImplemented


# def full_codings_generator_old(n, k=2):
#     if not k == 2:
#         raise NotImplemented
#     nodes = tuple(range(n))
#     partitions = tuple(partition(n, minimum=2))
#     for i_p, p_1 in enumerate(partitions):
#         c_1 = tuple(nodes[sum(p_1[:i]):sum(p_1[:i + 1])] for i, p in enumerate(p_1))
#         for p_2 in partitions[i_p:]:
#             if p_2 == (n,):
#                 continue
#             for c_2 in cycles(frozenset(nodes), p_2):
#                 yield (c_1, c_2), (p_1, p_2)
#     partition.cache_clear()
#     cycles.cache_clear()
#
#
# def half_codings_generator_old(n, k=2):
#     if not k == 2:
#         raise NotImplemented
#     nodes = tuple(range(n))
#     partitions = tuple(partition(n, minimum=2))
#     for i_p, p_1 in enumerate(partitions):
#         c_1 = tuple(nodes[sum(p_1[:i]):sum(p_1[:i + 1])] for i, p in enumerate(p_1))
#         succ = tuple(nodes[sum(p_1[:i]) + (j + 1) % p] for i, p in enumerate(p_1) for j in range(p))
#         # print(f"{c_1} - {succ}")
#         for p_2 in partitions[i_p:]:
#             if p_2 == (n,):
#                 continue
#             # print(f"\t{p_2}")
#             for c_2 in cycles_no_integer(frozenset(nodes), p_2, succ=succ):
#                 # print(f"\t\t{c_2}")
#                 yield (c_1, c_2), (p_1, p_2)
#     partition.cache_clear()
#     cycles_no_integer.cache_clear()
#     get_non_integer.cache_clear()
#
#
# def manual_codings_generator_old(n, k=2):
#     if not k == 2:
#         raise NotImplemented
#     nodes = tuple(range(n))
#     with open(var.GENERATOR_SETTINGS_FILEPATH.format(n=n, k=k), 'r') as f:
#         data = json.load(f)
#     parts = data['parts']
#     for p_1, p_2 in parts:
#         p_1, p_2 = tuple(p_1), tuple(p_2)
#         c_1 = tuple(nodes[sum(p_1[:i]):sum(p_1[:i + 1])] for i, p in enumerate(p_1))
#         succ = tuple(nodes[sum(p_1[:i]) + (j + 1) % p] for i, p in enumerate(p_1) for j in range(p))
#         for c_2 in cycles_no_integer(frozenset(nodes), p_2, succ=succ):
#             yield (c_1, c_2), (p_1, p_2)
#     cycles_no_integer.cache_clear()
#     get_non_integer.cache_clear()


CODINGS_GENERATORS = {
    'f': {
        'name': "full",
        'descr': "generates all possible graph codings",
        # 'func': full_codings_generator
        'func': (partitions_generator, "full")
    },
    'h': {
        'name': "half",
        'descr': "generates all graph codings with non-integer edges ",
        # 'func': half_codings_generator
        'func': (partitions_generator, "half")
    },
    'm': {
        'name': "manual half",
        'descr': "generates graph codings as described in the relative generator settings",
        # 'func': manual_codings_generator
        'func': (manual_partitions_generator, "half")
    },
}

"""
SPEED OF FULL GENERATORS WITH CACHING
 4  0.00010013580322265625
 5  0.00012636184692382812
 6  0.0008616447448730469
 7  0.003438234329223633
 8  0.011286497116088867
 9  0.13289237022399902
10  1.6032276153564453
11  48.75095868110657
"""

"""
COMPARISON OF GENERATED INSTANCES BY FULL AND HALF GENERATORS
  N      FULL      HALF  [expect]  ( perc )
--------------------------------------------
  4         3         2  [     2]  (66.67%)
  5        20         6  [     6]  (30.00%)
  6       215        68  [    68]  (31.63%)
  7      1974       628  [   628]  (31.81%)
  8     23786      7696  [  7696]  (32.36%)
  9    263640     86284  [ 86284]  (32.73%)
 10   3649635   1208098  [ ---- ]  (33.10%)
 11  48668730  16262299  [ ---- ]  (33.41%)
"""

if __name__ == '__main__':
    from modules.graph import Graph

    print("  N      FULL      HALF  [expect]  ( perc )\n--------------------------------------------")
    for n in range(4, 12):
        full = tuple(full_codings_generator(n))
        full_i = 0
        # for c, _ in full:
        #     g = Graph(n, 2, (1, 1), c)
        #     if g.vector.max() < 1:
        #         full_i += 1
        half = tuple(half_codings_generator(n))
        half_i = 0
        # for c, _ in half:
        #     g = Graph(n, 2, (1, 1), c)
        #     if g.vector.max() < 1:
        #         half_i += 1
        #     else:
        #         print(c)
        #         input()
        print(f" {n:>2}  {len(full):>8}  {len(half):>8}  [{full_i:>6}]  ({100 * len(half) / len(full):05.2f}%)")
