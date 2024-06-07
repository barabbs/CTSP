from itertools import permutations, combinations
from modules.coding import Coding, Cover
from functools import cache

import logging


@cache
def partition(n, maximum=float("inf"), minimum=2):
    if n <= maximum:
        yield (n,)
    for i in range(min(n - minimum, maximum), minimum - 1, -1):
        for tail in partition(n - i, i):
            yield (i, *tail)

@cache
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
            for c_2 in cycles(frozenset(nodes), p_2):
                yield Coding((c_1, c_2))


"""
4 3.2901763916015625e-05
5 4.3392181396484375e-05
6 0.0004787445068359375
7 0.0025649070739746094
8 0.06202578544616699
9 0.4634256362915039
10 4.7264580726623535
11 54.881959438323975"""