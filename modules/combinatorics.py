from itertools import permutations, combinations
from functools import cache

import logging


@cache
def partition(n, maximum=float("inf"), minimum=2):
    return (((n,),) if n <= maximum else tuple()) + sum(
        (tuple((i, *tail) for tail in partition(n - i, i)) for i in range(min(n - minimum, maximum), minimum - 1, -1)),
        start=tuple())


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


def full_codings_generator(n, k=2):
    if not k == 2:
        raise NotImplemented
    nodes = tuple(range(n))
    partitions = tuple(partition(n, maximum=n - 2, minimum=2))
    for i_p, p_1 in enumerate(partitions):
        c_1 = tuple(nodes[sum(p_1[:i]):sum(p_1[:i + 1])] for i, p in enumerate(p_1))
        for p_2 in partitions[i_p:]:
            for c_2 in cycles(frozenset(nodes), p_2):
                yield (c_1, c_2), (p_1, p_2)
    partition.cache_clear()
    cycles.cache_clear()


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
        if any(head[(i + 1) % parts[0]] == succ[v] for i, v in enumerate(head)):
            continue
        perms = tuple(permutations(head[1:]))
        reminder = nodes.difference(head)
        result += sum(
            (tuple(((top, *chunk), *tail) for chunk in perms) for tail in
             cycles_no_integer(reminder, parts[1:], succ=succ, minimum=(parts[0], top))),
            start=tuple())
    return result


def half_codings_generator(n, k=2):
    if not k == 2:
        raise NotImplemented
    nodes = tuple(range(n))
    partitions = tuple(partition(n, maximum=n - 2, minimum=2))
    for i_p, p_1 in enumerate(partitions):
        c_1 = tuple(nodes[sum(p_1[:i]):sum(p_1[:i + 1])] for i, p in enumerate(p_1))
        succ = tuple(nodes[sum(p_1[:i]) + (j + 1) % p] for i, p in enumerate(p_1) for j in range(p))
        for p_2 in partitions[i_p:]:
            for c_2 in cycles_no_integer(frozenset(nodes), p_2, succ=succ):
                yield (c_1, c_2), (p_1, p_2)
    partition.cache_clear()
    cycles_no_integer.cache_clear()


CODINGS_GENERATORS = {
    'f': {
        'name': "full",
        'descr': "generates all possible graph codings",
        'func': full_codings_generator
    },
    'h': {
        'name': "half",
        'descr': "generates all graph codings with non-integer edges ",
        'func': half_codings_generator
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
 4  0.00010013580322265625
 5  0.00012636184692382812
 6  0.0008616447448730469
 7  0.003438234329223633
 8  0.011286497116088867
 9  0.13289237022399902
10  1.6032276153564453
11  48.75095868110657
"""

if __name__ == '__main__':
    for n in range(6, 12):
        full = len(tuple(full_codings_generator(n)))
        half = len(tuple(half_codings_generator(n)))
        print(f"{n:>2}  {full:>8}  {half:>8}  ({100 * half / full:05.2f}%)")
