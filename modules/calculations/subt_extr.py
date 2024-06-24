from functools import cache
import numpy as np
from more_itertools import powerset
from .core import Calculation
import networkx as nx
from modules import var


@cache
def _get_base(n):
    nodes = tuple(range(n))
    return np.array(tuple((i, j) for i in nodes for j in nodes if i != j), dtype=int)


def _get_subset_vects(n):
    nodes = tuple(range(n))
    base = _get_base(n)
    subsets = filter(lambda S: 2 <= len(S) <= n - 2, powerset(nodes))
    vectors = np.array(tuple(tuple(u in S and v not in S for u, v in base) for S in subsets),
                       dtype=np.float32)
    return vectors


class SUBTEXTR_Base(Calculation):
    CALC_TYPE = var.CALC_SUBT_EXTR
    SUBT_CALC_NAME = None
    EXTR_CALC_NAME = None

    # def __init__(self, **kwargs):
    #     self.CALC_NAME = f"{self.SUBT_CALC_NAME} + {self.EXTR_CALC_NAME}"
    #     super().__init__(**kwargs)

    def _subt_calc(self, graph):
        raise NotImplemented

    def _extr_calc(self, graph, pre_calc):
        raise NotImplemented

    def _calc(self, graph):
        subt, active = self._subt_calc(graph)
        return {var.GRAPH_TABLE: {'prop_subt': subt, 'prop_extr': self._extr_calc(graph, active) if subt else None}}


class SUBT_Direct(SUBTEXTR_Base):
    SUBT_CALC_NAME = "Direct"

    def __init__(self, n, **kwargs):
        self.n = n
        super().__init__(n=n, **kwargs)
        self.subset_vects = _get_subset_vects(n)

    def _subt_calc(self, graph):
        graph_vect = graph.vector
        active = list()
        for subs_vect in self.subset_vects:
            res = np.dot(subs_vect, graph_vect)
            if res < 1.:
                return False, None
            elif res == 1.:
                active.append(subs_vect)
        return True, np.array(active)


class SUBT_Matrix(SUBT_Direct):
    SUBT_CALC_NAME = "Matrix"

    def _subt_calc(self, graph):
        res = np.matmul(self.subset_vects, graph.vector)
        if np.any(res < 1):
            return False, None
        active = self.subset_vects[res == 1., :]
        return True, active


class SUBT_MinCut(SUBTEXTR_Base):
    SUBT_CALC_NAME = "MinCut"

    def __init__(self, n, k, **kwargs):
        self.n, self.k = n, k
        super().__init__(n=n, k=k, **kwargs)
        self.subset_vects = _get_subset_vects(n)

    def _subt_calc(self, graph):
        digraph = graph.weighted_digraph
        for u in digraph.nodes:
            for v in digraph.nodes:
                if u == v:
                    continue
                new_val, new_cut = nx.minimum_cut(digraph, u, v, capacity='weight')
                if new_val < self.k:
                    return False, None
        active = self.subset_vects[np.matmul(self.subset_vects, graph.vector) == 1., :]
        return True, active


class EXTR_Matrix(SUBTEXTR_Base):
    EXTR_CALC_NAME = "Matrix"

    def __init__(self, n, **kwargs):
        self.n = n
        self.base = _get_base(n)
        super().__init__(n=n, **kwargs)
        self.bound_vects = np.identity((n * (n - 1)), dtype=np.float32)
        out_deg_vects = np.array(tuple(tuple(edge[0] == u for edge in self.base) for u in range(n)),
                                 dtype=np.float32)
        in_deg_vects = np.array(tuple(tuple(edge[1] == v for edge in self.base) for v in range(n)),
                                dtype=np.float32)
        self.deg_vects = np.concatenate((out_deg_vects, in_deg_vects), axis=0)

    def _extr_calc(self, graph, active_vects):
        active_bounds = self.bound_vects[graph.vector == 0., :]
        # print(
        #     f"\t\tprank {np.linalg.matrix_rank(np.vstack((active_bounds, self.deg_vects)))}/{(self.n * (self.n - 1))}")
        try:
            full_matrix = np.vstack((active_bounds, self.deg_vects, active_vects))
        except ValueError:
            return False
        rank = np.linalg.matrix_rank(full_matrix)
        # print(f"\t\trank  {rank}/{(self.n * (self.n - 1))}")
        return rank == (self.n * (self.n - 1))


# class EXTR_QuickMatrix(EXTR_Matrix):
#     EXTR_CALC_NAME = "Matrix (w/ quick check)"
#
#     def _extr_calc(self, graph, active_vects):
#         if np.max(graph.vector) == 1.:
#             return True
#         return super()._extr_calc(graph, active_vects)

# def pprint_ant(ants, gr, start="\t"):
#     if gr is None:
#         print(f"{start}        [{', '.join(str(s) for s in ants)}]")
#         return
#     print(f"{start}{gr}  [{', '.join(str(s) for s in ants[gr])}]")
#
#
# def add_antagonists_print(antagonists, exhausted, *groups):
#     if groups[0] == groups[1]:
#         # add_exhaust = groups[0]
#         iterator = (groups,)
#         # antagonists[gr].update(*(antagonists[gr] for gr in antagonists[gr])
#         # for g in antagonists[gr]:
#         #     antagonists[g] = antagonists[gr]
#     else:
#         exh_gr = set(groups).intersection(exhausted)
#         if len(exh_gr) > 0:
#             print(f"\n----  EXHAUST CASE  ----")
#             exh_gr, act_gr = exh_gr.pop(), set(groups).difference(exhausted).pop()
#             pprint_ant(antagonists, exh_gr, start="EXH ")
#             pprint_ant(antagonists, act_gr, start="ACT ")
#             # for gr in antagonists[act_gr]:
#             #     antagonists[exh_gr].update(antagonists[gr])
#             #     break
#             antagonists[act_gr].add(act_gr)
#             # add_exhaust = act_gr
#             pprint_ant(antagonists, act_gr, start="--> ")
#             iterator = ((exh_gr, act_gr),)
#         else:
#             # add_exhaust = None
#             iterator = (groups, groups[::-1])
#     for A, B in iterator:
#         print(f"\n----  A {A}\tB {B}  ----")
#         pprint_ant(antagonists, A, start="  A ")
#         pprint_ant(antagonists, B, start="  B ")
#         for gr in antagonists[B]:
#             pprint_ant(antagonists, gr)
#         #     antagonists[A].update(antagonists[gr])
#         #     break
#         antagonists[A].update(*(antagonists[gr] for gr in antagonists[B]))
#         pprint_ant(antagonists, A, start="NEW ")
#
#     print(f"\n--------  UPDATE  --------")
#     for A, B in iterator:
#         pprint_ant(antagonists, B, start="FRM ")
#         new_ants = antagonists[A]
#         for gr in antagonists[B]:
#             pprint_ant(antagonists, gr)
#             antagonists[gr] = new_ants
#         pprint_ant(antagonists, A, start=" TO ")
#         if A in new_ants:
#             exhausted.update(new_ants)
#     # if add_exhaust is not None:
#     #     add_exhausted(antagonists, exhausted, add_exhaust)


def add_exhausted(antagonists, exhausted, group):
    exhausted.update({group, }, antagonists[group])


def add_antagonists(antagonists, exhausted, *groups):
    if groups[0] == groups[1]:
        iterator = (groups,)
    else:
        exh_gr = set(groups).intersection(exhausted)
        if len(exh_gr) > 0:
            exh_gr, act_gr = exh_gr.pop(), set(groups).difference(exhausted).pop()
            antagonists[act_gr].add(act_gr)
            iterator = ((exh_gr, act_gr),)
        else:
            iterator = (groups, groups[::-1])
    for A, B in iterator:
        antagonists[A].update(*(antagonists[gr] for gr in antagonists[B]))
    for A, B in iterator:
        new_ants = antagonists[A]
        for gr in antagonists[B]:
            antagonists[gr] = new_ants
        if A in new_ants:
            exhausted.update(new_ants)


class EXTR_Chain(SUBTEXTR_Base):
    EXTR_CALC_NAME = "Chain"

    def __init__(self, n, k, **kwargs):
        self.n, self.k = n, k
        assert k == 2
        self.base = _get_base(n)
        super().__init__(n=n, k=k, **kwargs)
        self.bound_vects = np.identity((n * (n - 1)), dtype=np.float32)
        out_deg_vects = np.array(tuple(tuple(edge[0] == u for edge in self.base) for u in range(n)),
                                 dtype=np.float32)
        in_deg_vects = np.array(tuple(tuple(edge[1] == v for edge in self.base) for v in range(n)),
                                dtype=np.float32)
        self.deg_vects = np.concatenate((out_deg_vects, in_deg_vects), axis=0)

    def get_groups(self, graph_vect):
        support = set(tuple(i) for i in self.base[graph_vect == .5, :])
        groups, current = dict(), 0
        while len(support) > 0:
            last = support.pop()
            while True:
                groups[last] = (current, 0)
                inter = next(filter(lambda x: x[0] == last[0], support))
                support.remove(inter)
                groups[inter] = (current, 1)
                try:
                    last = next(filter(lambda x: x[1] == inter[1], support))
                except StopIteration:
                    break
                support.remove(last)
            current += 1
        return groups, current

    def _extr_calc(self, graph, active_vects):
        graph_vect = graph.vector
        groups, counter = self.get_groups(graph_vect)
        antagonists = dict(((gr, ind), {(gr, ind ^ 1), }) for gr in range(counter) for ind in (0, 1))
        exhausted = set()
        # print(f"\t\tgroup {group}")
        # i, j = 0, 0
        for vect in active_vects:
            # j += 1
            try:
                edge_1, edge_2 = self.base[np.multiply(vect, graph_vect) == .5, :]
            except ValueError:
                # i += 1
                continue
            group_1, group_2 = groups[tuple(edge_1)], groups[tuple(edge_2)]
            if group_2 not in antagonists[group_1] and (group_1 not in exhausted or group_2 not in exhausted):
                counter -= 1
                if counter == 0:
                    # return i, j
                    return True
                add_antagonists(antagonists, exhausted, group_1, group_2)
        # return i, j
        return False


_get_calc_name = lambda subt_class, extr_class: f"{subt_class.SUBT_CALC_NAME} + {extr_class.EXTR_CALC_NAME}"


class SUBTEXTR_Direct_Matrix(SUBT_Direct, EXTR_Matrix):
    CALC_NAME = _get_calc_name(SUBT_Direct, EXTR_Matrix)


# class SUBTEXTR_Direct_QuickMatrix(SUBT_Direct, EXTR_QuickMatrix):
#     CALC_NAME = _get_calc_name(SUBT_Direct, EXTR_QuickMatrix)


class SUBTEXTR_Direct_Chain(SUBT_Direct, EXTR_Chain):
    CALC_NAME = _get_calc_name(SUBT_Direct, EXTR_Chain)


class SUBTEXTR_Matrix_Matrix(SUBT_Matrix, EXTR_Matrix):
    CALC_NAME = _get_calc_name(SUBT_Matrix, EXTR_Matrix)


# class SUBTEXTR_Matrix_QuickMatrix(SUBT_Matrix, EXTR_QuickMatrix):
#     CALC_NAME = _get_calc_name(SUBT_Matrix, EXTR_QuickMatrix)


class SUBTEXTR_Matrix_Chain(SUBT_Matrix, EXTR_Chain):
    CALC_NAME = _get_calc_name(SUBT_Matrix, EXTR_Chain)


class SUBTEXTR_MinCut_Matrix(SUBT_MinCut, EXTR_Matrix):
    CALC_NAME = _get_calc_name(SUBT_MinCut, EXTR_Matrix)


# class SUBTEXTR_MinCut_QuickMatrix(SUBT_MinCut, EXTR_QuickMatrix):
#     CALC_NAME = _get_calc_name(SUBT_MinCut, EXTR_QuickMatrix)


class SUBTEXTR_MinCut_Chain(SUBT_MinCut, EXTR_Chain):
    CALC_NAME = _get_calc_name(SUBT_MinCut, EXTR_Chain)


CALCULATIONS_LIST = (SUBTEXTR_Matrix_Chain,  # 1
                     # SUBTEXTR_Matrix_QuickMatrix,  # 3
                     SUBTEXTR_Matrix_Matrix,  # 5
                     SUBTEXTR_Direct_Chain,  # 2
                     # SUBTEXTR_Direct_QuickMatrix,  # 4
                     SUBTEXTR_Direct_Matrix,  # 6
                     SUBTEXTR_MinCut_Chain,
                     # SUBTEXTR_MinCut_QuickMatrix,
                     SUBTEXTR_MinCut_Matrix,)

# n = 10
# 5. Matrix + Chain
# 2. Direct + Chain
# 4. Matrix + Matrix (w/ quick check)
# 1. Direct + Matrix (w/ quick check)
# 3. Matrix + Matrix
# 0. Direct + Matrix
# 6. MinCut + Matrix
# 7. MinCut + Matrix (w/ quick check)
# 8. MinCut + Chain

# n = 9
# 5. Matrix + Chain
# 4. Matrix + Matrix (w/ quick check)
# 3. Matrix + Matrix
# 2. Direct + Chain
# 1. Direct + Matrix (w/ quick check)
# 0. Direct + Matrix
# 6. MinCut + Matrix
# 8. MinCut + Chain
# 7. MinCut + Matrix (w/ quick check)

# n=8
# 5. Matrix + Chain
# 4. Matrix + Matrix (w/ quick check)
# 2. Direct + Chain
# 3. Matrix + Matrix
# 1. Direct + Matrix (w/ quick check)
# 0. Direct + Matrix
# 8. MinCut + Chain
# 6. MinCut + Matrix
# 7. MinCut + Matrix (w/ quick check)

# n=7
# 5. Matrix + Chain
# 2. Direct + Chain
# 4. Matrix + Matrix (w/ quick check)
# 1. Direct + Matrix (w/ quick check)
# 3. Matrix + Matrix
# 0. Direct + Matrix
# 6. MinCut + Matrix
# 7. MinCut + Matrix (w/ quick check)
# 8. MinCut + Chain

# n=6
# 5. Matrix + Chain
# 2. Direct + Chain
# 1. Direct + Matrix (w/ quick check)
# 4. Matrix + Matrix (w/ quick check)
# 3. Matrix + Matrix
# 0. Direct + Matrix
# 7. MinCut + Matrix (w/ quick check)
# 8. MinCut + Chain
# 6. MinCut + Matrix
