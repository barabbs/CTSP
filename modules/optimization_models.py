import pyomo.environ as pyo
import pyomo.core
from more_itertools import powerset
from itertools import permutations
import logging
import numpy as np
from modules import utility as utl

pyomo.core.base.indexed_component.normalize_index.flatten = False
logging.getLogger('pyomo.core').setLevel(logging.ERROR)

PYOMO_SOLVER = pyo.SolverFactory("gurobi")


def _get_edges(model):
    for i in model.nodes:
        for j in model.nodes:
            if i != j:
                yield (i, j)


def _get_nodes_subsets(model):
    for S in powerset(model.nodes):
        if 2 <= len(S) <= model.n - 2:
            yield (S,)


def _get_out_degree_constr(model, u):
    return sum(model.x[(u, v)] for v in model.nodes if v != u) == 1


def _get_in_degree_constr(model, v):
    return sum(model.x[(u, v)] for u in model.nodes if u != v) == 1


def _get_subtour_elimination_constr(model, S):
    return sum(sum(model.x[(u, v)] for u in S) for v in filter(lambda i: i not in S, model.nodes)) >= 1


def _get_SEP_objective(model):
    return pyo.summation(model.c, model.x)


class SEPModel(pyo.AbstractModel):
    def __init__(self):
        super().__init__()
        self._init_model()

    def _init_model(self):
        # Sets
        self.n = pyo.Param(within=pyo.PositiveIntegers)
        self.nodes = pyo.RangeSet(0, self.n - 1)
        # self.nodes = pyo.Set(dimen=1, within=pyo.NonNegativeIntegers)
        self.edges = pyo.Set(dimen=2, within=self.nodes * self.nodes, initialize=_get_edges)
        self.nodes_subs = pyo.Set(dimen=1, initialize=_get_nodes_subsets)

        # Costs and Variables
        self.x = pyo.Var(self.edges, within=pyo.NonNegativeReals)
        self.c = pyo.Param(self.edges, within=pyo.NonNegativeReals, initialize=1)

        self.out_degree_constr = pyo.Constraint(self.nodes, rule=_get_out_degree_constr)
        self.in_degree_constr = pyo.Constraint(self.nodes, rule=_get_in_degree_constr)
        self.subtour_elimination_constr = pyo.Constraint(self.nodes_subs, rule=_get_subtour_elimination_constr)

        self.objective = pyo.Objective(rule=_get_SEP_objective)

    def check_subt_extr(self, n, adj):
        inst = self.create_instance(data={None: {"n": {None: n}}})
        inst.x.set_values(adj)  # index:value
        active_coeffs = list()
        for S in inst.nodes_subs:
            constr = inst.subtour_elimination_constr[S]
            # print(constr.name, constr.slack())
            # constr.pprint()  # show the construction of c
            # print(" | ".join(f"{i[0]},{i[1]}" for i in inst.edges))
            # print(" | ".join(f" {int(i)} " for i in utl.get_vector_from_constraint(constr, inst.edges)))
            slack = constr.slack()
            if slack == 0:
                active_coeffs.append(utl.get_vector_from_constraint(constr, inst.edges))
            elif slack < 0:
                return False, None

        # If this is reached, all subtour elimination constraints are met.
        # Now checking for extremality of the solution

        for constr_group in (inst.in_degree_constr, inst.out_degree_constr):
            for u in inst.nodes:
                active_coeffs.append(utl.get_vector_from_constraint(constr_group[u], inst.edges))

        # TODO: Replace above with this
        # for u in inst.nodes:
        #     active_coeffs.append(utl.get_vector_from_variables(((u, v) for v in inst.nodes if v != u), inst.edges))
        #     active_coeffs.append(utl.get_vector_from_variables(((v, u) for v in inst.nodes if v != u), inst.edges))

        for edge in inst.edges:
            if adj[edge] == 0 or adj[edge] == 1:   # TODO: Remove check for <= 1
                active_coeffs.append(utl.get_vector_from_variables((edge,), inst.edges))

        # print("\n".join(f"{i:>2}    " + " ".join(str(int(s)) for s in r) for i, r in enumerate(active_coeffs)))
        rank = int(np.linalg.matrix_rank(active_coeffs))
        # print(f"RANK {rank}/{n * (n - 1)}")
        return True, rank == (n * (n - 1))


def _get_triplets(model):
    for i in model.nodes:
        for j in model.nodes:
            if i != j:
                for k in model.nodes:
                    if k != i and k != j:
                        yield (i, j, k)


def _get_tours(model):
    yield from permutations(range(1, len(model.nodes)))


def _get_metric_constr(model, u, v, w):
    return model.c[(u, w)] + model.c[(w, v)] - model.c[(u, v)] >= 0


def _get_tour_cost_constr(model, *tour):
    return model.c[(0, tour[0])] + sum(model.c[(u, v)] for u, v in zip(tour, tour[1:])) + model.c[(tour[-1], 0)] >= 1


def _get_dual_constr(model, u, v):
    S_x = tuple(filter(lambda S: u in S[0] and v not in S[0],
                       model.nodes_subs))  # TODO: replace filter with constructive generation (combinations)
    expr = model.c[(u, v)] - model.y[(u, 0)] - model.y[(v, 1)] - sum(model.d[S] for S in S_x)
    if model.x[(u, v)] > 0:
        return expr == 0
    else:
        return expr >= 0


def _get_OPT_objective(model):
    return pyo.summation(model.x, model.c)


class GAPModel(pyo.AbstractModel):
    def __init__(self):
        super().__init__()
        self._init_model()

    def _init_model(self):
        # Sets
        self.n = pyo.Param(within=pyo.PositiveIntegers)
        self.nodes = pyo.RangeSet(0, self.n - 1)
        # self.nodes = pyo.Set(dimen=1, within=pyo.NonNegativeIntegers)
        self.edges = pyo.Set(dimen=2, within=self.nodes * self.nodes, initialize=_get_edges)
        self.nodes_subs = pyo.Set(initialize=_get_nodes_subsets)

        # Auxiliary sets
        self.triplets = pyo.Set(dimen=3, initialize=_get_triplets)
        self.tours = pyo.Set(initialize=_get_tours)

        # Variables and Params
        self.x = pyo.Param(self.edges, within=pyo.NonNegativeReals)
        self.c = pyo.Var(self.edges, within=pyo.NonNegativeReals)
        self.y = pyo.Var(self.nodes * pyo.Binary, within=pyo.Reals)  # 2nd index: out = 0, in = 1
        self.d = pyo.Var(self.nodes_subs, within=pyo.NonNegativeReals)

        self.metric_constr = pyo.Constraint(self.triplets, rule=_get_metric_constr)
        self.tour_cost_constr = pyo.Constraint(self.tours, rule=_get_tour_cost_constr)
        self.dual_constr = pyo.Constraint(self.edges, rule=_get_dual_constr)

        self.objective = pyo.Objective(rule=_get_OPT_objective)

    def solve_instance(self, n, adj, verbose=False):
        inst = self.create_instance(data={None: {"n": {None: n}, "x": adj}})
        sol = PYOMO_SOLVER.solve(inst, tee=verbose)
        val, raw = pyo.value(inst.objective), sol.json_repn()['Solver'][0]
        return 1 / val, raw


SEP_MODEL = SEPModel()
GAP_MODEL = GAPModel()

if __name__ == '__main__':
    def get_vals(N, arcs, values=None):
        cost = dict()
        if values is None:
            values = (0.5,) * len(arcs)
            # values = (1,) * len(arcs)
        for i in range(N):
            for j in range(N):
                if i != j:
                    cost[(i, j)] = 0

        for val, arc in zip(values, arcs):
            cost[arc] = val
        return cost


    def _print_costs(model, mult=None, auto=True):
        print(f" i\\j  " + "".join(f"{i:<6}" for i in model.nodes) + "\n    ┌" + "─" * 6 * len(model.nodes))
        minimum = float("inf")
        for i in model.nodes:
            print(f" {i:<3}│ ", end="")
            for j in model.nodes:
                if i == j:
                    val = "-"
                else:
                    if mult is not None:
                        val = model.c[(i, j)]() * mult
                        print(f"{round(val):<6}", end="")
                    else:
                        val = model.c[(i, j)]()
                        if val != 0:
                            minimum = min(val, minimum)
                        print(f"{round(val, 3):<6}", end="")
            print()
        if mult is None and auto:
            mult = round(1 / minimum)
            print(f"\n >>> Printing again with auto multiplier {mult}")
            _print_costs(model, mult=mult)


    def _print_x(model, mult=2):
        print(f" i\\j  " + "".join(f"{i:<6}" for i in model.nodes) + "\n    ┌" + "─" * 6 * len(model.nodes))
        for i in model.nodes:
            print(f" {i:<3}│ ", end="")
            for j in model.nodes:
                if i == j:
                    val = "-"
                else:
                    val = model.x[(i, j)] * mult
                    val = round(val)
                print(f"{val:<6}", end="")
            print()


    model = GAPModel()

    # N = 4
    # vals = get_vals(N, ((0, 1), (1, 0), (1, 2), (2, 0), (2, 3), (3, 2), (3, 1), (0, 3)))
    # res = 1 / (6 / 5)

    # N = 5
    # vals = get_vals(N, ((0, 1), (1, 0), (1, 2), (2, 0), (2, 3), (3, 2), (3, 4), (4, 3), (4, 1), (0, 4)))
    # res = 1 / (5 / 4)

    # N = 6
    # vals = get_vals(N, ((0, 1), (1, 0), (1, 2), (2, 1), (2, 3), (3, 0), (3, 4), (4, 3), (4, 5), (5, 4), (5, 2), (0, 5)))
    # res = 1 / (4 / 3)

    # N = 8
    # vals = get_vals(N, ((0, 1), (1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 7), (7, 0),
    #                     (0, 6), (6, 4), (4, 2), (2, 0), (7, 5), (5, 3), (3, 1), (1, 7)), (0.25,) * 8 + (0.75,) * 8)
    # res = 1 / (4 / 3)

    N = 9
    vals = get_vals(N, (
        (0, 1), (1, 0), (2, 3), (3, 2), (3, 4), (4, 3), (5, 6), (6, 5), (6, 7), (7, 6), (0, 2), (2, 8), (8, 7), (7, 1),
        (4, 0), (8, 4), (5, 8), (1, 5)))
    res = 1 / (11 / 8)

    data = {None: {"n": {None: N},
                   "x": vals}}
    inst = model.create_instance(data=data)
    # inst.pprint()

    solver = pyo.SolverFactory("gurobi")
    sol = solver.solve(inst, tee=True)

    print(f"EXPECTED RESULT: {res}")
    print("GRAPH")
    _print_x(inst, mult=2)
    print("\nCOSTS")
    _print_costs(inst, mult=None)
    # print(sol)
