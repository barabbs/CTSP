import gurobipy as gp
from gurobipy import GRB
from more_itertools import powerset
from itertools import permutations
from .core import Calculation
from modules import var
import os, logging


class GAP_Gurobi(Calculation):
    CALC_TYPE = var.CALC_GAP
    CALC_NAME = "Gurobi"

    def __init__(self, n, gurobi_verbose=False, gurobi_reset=True, **kwargs):
        self.n = n
        self.verbose, self.reset = gurobi_verbose, gurobi_reset
        # Sets
        self.nodes = tuple(range(self.n))
        self.edges = tuple((i, j) for i in self.nodes for j in self.nodes if i != j)
        self.triplets = tuple((i, j, k) for i, j in self.edges for k in self.nodes if k != i and k != j)
        self.nodes_subs = tuple(filter(lambda S: 2 <= len(S) <= self.n - 2, powerset(self.nodes)))
        self.tours = tuple(permutations(range(1, self.n)))
        super().__init__(**kwargs)

    def _get_tour_cost_constr(self, tour):
        return self.c[(0, tour[0])] + sum(self.c[(u, v)] for u, v in zip(tour, tour[1:])) + self.c[(tour[-1], 0)]

    def _get_dual_constr(self, u, v):
        S_x = filter(lambda S: u in S and v not in S,
                     self.nodes_subs)  # TODO: replace filter with constructive generation (combinations)
        return self.c[(u, v)] - self.y[(u, 0)] - self.y[(v, 1)] - sum(self.d[S] for S in S_x)

    def _init_model(self):
        print(f"Initializig gurobi model in process {os.getpid() - os.getppid():<4}")
        self.env = gp.Env(params={"OutputFlag": int(self.verbose)})
        self.model = gp.Model(env=self.env)

        # Variables
        self.c = self.model.addVars(self.edges, name='c')
        self.y = self.model.addVars(self.nodes, (0, 1), lb=float('-inf'), name='y')
        self.d = self.model.addVars(self.nodes_subs, name='d')

        # Constraints
        self.metric_constr = self.model.addConstrs(
            (self.c[(u, w)] + self.c[(w, v)] - self.c[(u, v)] >= 0 for u, v, w in self.triplets),
            name="metric")
        self.tour_cost_constr = self.model.addConstrs(
            (self._get_tour_cost_constr(tour) >= 1 for tour in self.tours),  # TODO: Increase Laziness of consraint?
            name="tour_cost")
        self.dual_constr = self.model.addConstrs(
            (self._get_dual_constr(*edges) >= 0 for edges in self.edges),
            name="dual")

        # self.model.Params.OutputFlag = int(self.verbose)
        return self.model

    def __getattr__(self, item):
        if item == 'model':
            return self._init_model()
        raise AttributeError

    def _calc(self, graph):
        model = self.model
        print(f"    {os.getpid() - os.getppid():<4}")

        if self.reset:
            model.reset()
        for u, v, value in graph.edge_count_generator(weight=True):
            self.c[(u, v)].Obj = value
            self.dual_constr[(u, v)].Sense = '=' if value > 0 else '>'
        model.optimize()
        return {var.GRAPH_TABLE: {'gap': 1 / model.objVal}}
        # return {var.GRAPH_TABLE: {'gap': 1 / self.model.objVal}, var.GAP_INFO_TABLE: utl.convert_raw_gurobi_info(raw_info)}


CALCULATIONS_LIST = (GAP_Gurobi,)

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
    model = GAP_Gurobi(n=N, verbose=True)

    sol = model._calc(vals)

    print(f"EXPECTED RESULT: {res}")
    # print("GRAPH")
    # _print_x(inst, mult=2)
    # print("\nCOSTS")
    # _print_costs(inst, mult=None)
    # print(sol)
