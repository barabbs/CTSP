from itertools import permutations, combinations
from modules.coding import Coding, Cover

import logging


def partition(n, maximum=float("inf"), minimum=2):
    if n <= maximum:
        yield (n,)
    for i in range(min(n - minimum, maximum), minimum - 1, -1):
        for tail in partition(n - i, i):
            yield (i, *tail)


def cycles(nodes, parts, minimum=None):
    if len(parts) == 0:
        yield tuple()
        return
    min_check = minimum is not None and parts[0] == minimum[0]
    for head in combinations(nodes, parts[0]):
        top = head[0]
        if min_check and top < minimum[1]:
            continue
        perms = tuple(permutations(head[1:]))
        reminder = nodes.difference(head)
        for tail in cycles(reminder, parts[1:], minimum=(parts[0], top)):
            for chunk in perms:
                yield ((top, *chunk), *tail)


def graph_codings_generator(n, k=2):
    if not k == 2:
        raise NotImplemented
    nodes = tuple(range(n))
    partitions = tuple(partition(n, maximum=n - 2, minimum=2))
    for i_p, p_1 in enumerate(partitions):
        c_1 = tuple(nodes[sum(p_1[:i]):sum(p_1[:i + 1])] for i, p in enumerate(p_1))
        for p_2 in partitions[i_p:]:
            for c_2 in cycles(set(nodes), p_2):
                yield Coding((c_1, c_2))
