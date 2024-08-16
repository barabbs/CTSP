import gurobipy as gp
from gurobipy import GRB
from more_itertools import powerset
from itertools import permutations
from .core import Calculation
from modules import var
from modules import utility as utl
import os, time, logging


class GAP_Gurobi(Calculation):
    CALC_TYPE = var.CALC_GAP
    CALC_NAME = "Gurobi"

    def __init__(self, n, gurobi_verbose=False, gurobi_reset=0,
                 gurobi_method=-1, gurobi_presolve=-1, gurobi_pre_sparsify=-1, gurobi_threads=None,
                 # presolve_queue=None,
                 **kwargs):
        self.n = n
        self.verbose, self.reset = gurobi_verbose, gurobi_reset
        self.method, self.presolve, self.pre_sparsify, self.threads = gurobi_method, gurobi_presolve, gurobi_pre_sparsify, gurobi_threads or 0
        # self.presolve_queue = presolve_queue
        self.callback = None
        # Sets
        self.nodes = tuple(range(self.n))
        self.edges = tuple((i, j) for i in self.nodes for j in self.nodes if i != j)
        self.triplets = tuple((i, j, k) for i, j in self.edges for k in self.nodes if k != i and k != j)
        self.nodes_subs = tuple(filter(lambda S: 2 <= len(S) <= self.n - 2, powerset(self.nodes)))
        self.tours = tuple(permutations(range(1, self.n)))
        super().__init__(**kwargs)

    def _initialize(self):
        self.model

    def _close(self):
        self.model.dispose()
        self.env.dispose()
        del self.model

    def _get_tour_cost_constr(self, tour):
        return self.c[(0, tour[0])] + sum(self.c[(u, v)] for u, v in zip(tour, tour[1:])) + self.c[(tour[-1], 0)]

    def _get_dual_constr(self, u, v):
        S_x = filter(lambda S: u in S and v not in S,
                     self.nodes_subs)  # TODO: replace filter with constructive generation (combinations)
        return self.c[(u, v)] - self.y[(u, 0)] - self.y[(v, 1)] - sum(self.d[S] for S in S_x)

    def _init_model(self):
        # print(f"{os.getpid() - os.getppid():<4} - Initializing gurobi model")
        self.env = gp.Env(params={"OutputFlag": int(self.verbose), "Threads": self.threads,
                                  "Presolve": self.presolve, "Method": self.method, "PreSparsify": self.pre_sparsify})
        # self.env = gp.Env(params={"OutputFlag": int(self.verbose), "Presolve": self.presolve})
        self.model = gp.Model(env=self.env)

        # CREATING VARIABLES AND CONSTRAINTS TAKES UP A LOT OF TIME!
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
        # print(f"{os.getpid() - os.getppid():<4} - GETATTR {item}")
        if item == 'model':
            return self._init_model()
        raise AttributeError

    def _update_obj_constr(self, graph):
        for u, v, value in graph.edge_count_generator(weight=True):
            self.c[(u, v)].Obj = value
            self.dual_constr[(u, v)].Sense = '=' if value > 0 else '>'

    def _calc(self, graph):
        # print(f"    {os.getpid() - os.getppid():<4}")
        if self.reset is not None:
            self.model.reset(self.reset)
        self._update_obj_constr(graph)
        # if self.presolve_queue is not None:
        #     self.model.presolve()
        #     self.presolve_queue.put(time.process_time_ns())
        self.model.optimize(callback=self.callback)
        if self.model.Status == GRB.OPTIMAL:  # Optimal solution found
            return {var.GRAPH_TABLE: {'gap': 1 / self.model.objVal}}
        elif self.model.Status == GRB.INFEASIBLE or self.model.Status == GRB.INTERRUPTED:
            return {var.GRAPH_TABLE: {'gap': 0}}
        logging.warning(f"Status {self.model.Status} of optimization for graph {graph} not recognized!")


BOUND_THRESHOLD = 1e-15


class GAP_Gurobi_Bound_Base(GAP_Gurobi):
    CALC_TYPE = var.CALC_GAP

    def __init__(self, n, obj_bound=None, **kwargs):
        self.obj_bound = obj_bound or utl.get_best_gap(n - 1)
        if self.obj_bound is not None:
            self.obj_bound = (1 / self.obj_bound) - BOUND_THRESHOLD
        self.obj_bound_constr = None
        super().__init__(n=n, **kwargs)


class GAP_Gurobi_Bound_Constr(GAP_Gurobi_Bound_Base):
    CALC_NAME = "Gurobi w/ Bound (constraint)"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.obj_bound_constr = None

    def _update_obj_constr(self, graph):
        super()._update_obj_constr(graph)
        if self.obj_bound_constr is not None:
            self.model.remove(self.obj_bound_constr)
        if self.obj_bound is not None:
            self.obj_bound_constr = self.model.addConstr(
                gp.LinExpr(tuple((value, self.c[(u, v)]) for u, v, value in
                                 graph.edge_count_generator(weight=True))) <= self.obj_bound,
                name="obj_bound")
            # self.obj_bound_constr = self.model.addConstr(
            #     gp.quicksum((value * self.c[(u, v)] for u, v, value in
            #                  graph.edge_count_generator(weight=True))) < self.obj_bound,
            #     name="obj_bound")


class GAP_Gurobi_Bound_Callback(GAP_Gurobi_Bound_Base):
    CALC_NAME = "Gurobi w/ Bound (callback)"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.callback = self._get_callback()

    def _get_callback(self):
        # def callback(model, where):
        #     print(f"Callback {where} - {getattr(model, "ObjBound", "-")}")
        #     if where == GRB.Callback.MIP:
        #         objbnd = model.cbGet(GRB.Callback.MIP_OBJBND)
        #         print(f"MIP {objbnd}")
        #     elif where == GRB.Callback.MIPNODE:
        #         objbnd = model.cbGet(GRB.Callback.MIPNODE_OBJBND)
        #         print(f"MIPNODE {objbnd}")
        #     elif where == GRB.Callback.MIPSOL:
        #         objbnd = model.cbGet(GRB.Callback.MIPSOL_OBJBND)
        #         print(f"MIPSOL! {objbnd}")
        #     else:
        #         return
        #     if objbnd > self.obj_bound:
        #         model.terminate()
        if self.obj_bound is not None:
            def callback(model, where):
                # print(f"Callback {where} - {getattr(model, "ObjBound", "-")}")
                # print(getattr(model, "ObjBound", "-"))
                if getattr(model, "ObjBound", 0) > self.obj_bound:
                    model.terminate()

            return callback
        return None


CALCULATIONS_LIST = (
    GAP_Gurobi_Bound_Callback,
    GAP_Gurobi,
    GAP_Gurobi_Bound_Constr,
)

"""
PERFORMANCE OF CALCULATIONS ON EXAMPLES  (Presolve = -1)

Gurobi                              Gurobi w/ Bound (callback)          Gurobi w/ Bound (constraint)      
------------------------------------------------------------------------------------------------------------
2.00e+09 ± 1.2e+08  (+0.0e+00)      2.13e+09 ± 2.3e+07  (+1.3e+08)      2.24e+09 ± 2.2e+07  (+2.3e+08)      
1.14e+10 ± 2.1e+09  (+0.0e+00)      5.20e+09 ± 3.7e+09  (-6.2e+09)      9.64e+09 ± 2.9e+09  (-1.8e+09)      
1.21e+10 ± 1.4e+09  (+0.0e+00)      9.99e+09 ± 3.0e+09  (-2.2e+09)      1.46e+10 ± 2.0e+09  (+2.4e+09)      
1.48e+10 ± 1.9e+09  (+0.0e+00)      1.18e+10 ± 2.3e+09  (-3.0e+09)      1.89e+10 ± 2.7e+09  (+4.2e+09)      
1.09e+10 ± 4.2e+09  (+0.0e+00)      9.99e+09 ± 1.6e+09  (-8.8e+08)      1.27e+10 ± 2.7e+09  (+1.8e+09)      
1.39e+10 ± 5.3e+09  (+0.0e+00)      7.75e+09 ± 3.3e+09  (-6.2e+09)      2.00e+10 ± 2.2e+09  (+6.0e+09)      
1.03e+10 ± 2.2e+09  (+0.0e+00)      1.02e+10 ± 3.4e+09  (-1.1e+08)      1.49e+10 ± 6.8e+09  (+4.6e+09)      
1.97e+10 ± 3.3e+09  (+0.0e+00)      1.62e+10 ± 3.8e+09  (-3.5e+09)      5.12e+09 ± 2.1e+09  (-1.5e+10)      
1.97e+10 ± 2.5e+09  (+0.0e+00)      1.40e+10 ± 2.4e+09  (-5.6e+09)      9.93e+09 ± 1.7e+09  (-9.7e+09)      
1.56e+10 ± 2.3e+09  (+0.0e+00)      1.28e+10 ± 1.9e+09  (-2.9e+09)      3.00e+10 ± 3.1e+09  (+1.4e+10)      
1.78e+10 ± 2.3e+09  (+0.0e+00)      1.56e+10 ± 2.0e+09  (-2.2e+09)      4.15e+10 ± 3.8e+09  (+2.4e+10)      
2.01e+09 ± 3.2e+07  (+0.0e+00)      1.97e+09 ± 3.2e+07  (-4.0e+07)      3.33e+10 ± 5.1e+09  (+3.1e+10)      
"""

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
