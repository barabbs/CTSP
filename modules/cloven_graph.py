import matplotlib.pyplot as plt
import networkx as nx
import pynauty
from modules.combinatorics import *
from modules.coding import Coding, Cover
from modules import optimization_models as opt_mdl
import modules.utility as utl
from modules import graphics
from modules import var
import time

PROPERTIES = (
    'subt',  # graph satisfies subtour elimination constraints
    'extr',  # graph is extremal solution of SEP
    'canon',  # graph code is canonical
    # 'subt_old',  # graph satisfies subtour elimination (LEGACY)
    # 'extr_old',  # graph is extremal solution of SEP   (LEGACY)
    # 'sort',  # graph code is sorted                    (LEGACY)
    # 'plan'  # graph is planar                          (LEGACY)
)

from sqlalchemy.orm import DeclarativeBase, reconstructor
from sqlalchemy import Integer, LargeBinary, Boolean, Float, String, PickleType, ForeignKey
from sqlalchemy.orm import mapped_column, relationship


class Base(DeclarativeBase):
    pass


def calc_certificate(nauty_graph):
    return utl.timing(pynauty.certificate, nauty_graph)


def calc_gap(n, adj_matrix, opt_verbose):
    def calc_gap_wrap(adj_matrix):
        return opt_mdl.GAP_MODEL.solve_instance(n, adj_matrix, opt_verbose)

    return utl.timing(calc_gap_wrap, adj_matrix)


def check_subt_extr(n, adj_matrix):
    def check_subt_extr_wrap(adj_matrix):
        return opt_mdl.SEP_MODEL.check_subt_extr(n, adj_matrix)

    return utl.timing(check_subt_extr_wrap, adj_matrix)


def check_canon(coding):
    def check_canon_wrap(coding):
        for j, comp in enumerate(coding.code_permutations()):
            for i, trans in enumerate(comp[0].get_translations()):
                if coding.apply_translation(trans) < coding:
                    return False
        return True

    return utl.timing(check_canon_wrap, coding)


def get_ClovenGraph(N, K, W):
    class ClovenGraph(Base):
        __tablename__ = "graphs"

        # n = mapped_column(Integer, nullable=False)
        # k = mapped_column(Integer, nullable=False)
        # weights = mapped_column(PickleType, nullable=False, primary_key=True)
        n = N
        k = K
        weights = W
        coding = mapped_column(PickleType, nullable=False, primary_key=True)
        parts = mapped_column(PickleType, nullable=False)
        certificate = mapped_column(LargeBinary, nullable=True)
        gap = mapped_column(Float, nullable=True)
        prop_subt = mapped_column(Boolean, nullable=True)
        prop_extr = mapped_column(Boolean, nullable=True)
        prop_canon = mapped_column(Boolean, nullable=True)
        gap_info = relationship("GAPInfo", back_populates="graph", lazy="immediate", uselist=False)
        timings = relationship("Timings", back_populates="graph", lazy="immediate", uselist=False)

        def __init__(self, coding, **kwargs):
            # self.n, self.k, self.weights = n, k, weights
            parts = tuple(tuple(len(c) for c in cover) for cover in coding)
            super().__init__(coding=coding, parts=parts, **kwargs)
            Timings(coding=self.coding, graph=self)
            self._init_on_load()

        @reconstructor
        def _init_on_load(self):
            self._coding = Coding(Cover(cycles=c, weight=w) for c, w in zip(self.coding, self.weights))
            self.graph = utl.get_graph_from_code(self._coding)
            self.nauty_graph, self.adjacency_matrix = None, None
            # self.nauty_graph = utl.get_nauty_graph(self.n, self.k, self.graph)
            # self.adjacency_matrix = utl.get_adjacency_matrix(self.n, self.k, self.graph, self.weights)

        @staticmethod
        def timing(time_type):
            def timing_decorator(function):
                def wrapper(self, *args, **kwargs):
                    start = time.process_time_ns()
                    result = function(self, *args, **kwargs)
                    setattr(self.timings, time_type, time.process_time_ns() - start)
                    return result

                return wrapper

            return timing_decorator

        # PRIVATE UTILITIES

        def _apply_translation(self, translation):
            return tuple(cover.apply_translation(translation) for cover in self._coding)

        def _edges_in_subgraph(self, s):
            return self.graph.subgraph(s).size()

        def get_nauty_graph(self):
            if self.nauty_graph is None:
                self.nauty_graph = utl.get_nauty_graph(self.n, self.k, self.graph)
            return self.nauty_graph

        def get_adjacency_matrix(self):
            if self.adjacency_matrix is None:
                self.adjacency_matrix = utl.get_adjacency_matrix(self.n, self.k, self.graph, self.weights)
            return self.adjacency_matrix

        def set_property(self, prop_name, value):
            setattr(self, f"prop_{prop_name}", value)

        def get_property(self, prop_name):
            return getattr(self, f"prop_{prop_name}")

        def set_timing(self, time_type, exec_time):
            setattr(self.timings, time_type, exec_time)

        def set_gap(self, gap, raw, session):
            self.gap = gap
            session.add(GAPInfo(coding=self.coding, graph=self, raw=raw))

        # CALCULATIONS

        @timing('calc_certificate')
        def calc_certificate(self):
            self.certificate = pynauty.certificate(self.get_nauty_graph())

        # @timing('calc_canonic_code')  # TODO: Not implemented!
        # def calc_canonic_code(self):
        #     code = self._coding
        #     # print(f"        BEGIN {code}")
        #     for j, comp in enumerate(self._coding.code_permutations()):
        #         # print(f"            COMP {comp}")
        #         for i, trans in enumerate(comp[0].get_translations()):
        #             new_code = self._coding.apply_translation(trans)
        #             # print(f"                {new_code}        {trans}")
        #             code = min(code, new_code)
        #     return code

        @timing('calc_gap')
        def calc_gap(self):
            self.set_gap(*opt_mdl.GAP_MODEL.solve_instance(self.n, self.get_adjacency_matrix()))

        # PROPERTIES CHECK

        @timing('prop_subt_extr')
        def check_subt_extr(self):
            subt, extr = opt_mdl.SEP_MODEL.check_subt_extr(self.n, self.get_adjacency_matrix())
            self.set_property('subt', subt)
            self.set_property('extr', extr)

        @timing('prop_canon')
        def check_canon(self):
            code = self._coding
            for j, comp in enumerate(self._coding.code_permutations()):
                for i, trans in enumerate(comp[0].get_translations()):
                    if self._coding.apply_translation(trans) < code:
                        self.set_property('canon', False)
                        return
            self.set_property('canon', True)

        # @timing('prop_subt_extr_old')  # TODO: Not implemented!
        # def check_subt_extr_old(self):  # TODO: CHECK FOR CONSISTENCY AND SPEED
        #     # TODO: Improve algorithm taking components into account
        #     check_for_extr = self.properties['extr_old'] is None
        #     for k in range(2, self.n - 1):
        #         max_size = 2 * (k - 1)
        #         for s in combinations(self.graph.nodes, k):
        #             size = self._edges_in_subgraph(s)
        #             if size > max_size:
        #                 self.properties['subt_old'] = False
        #                 # self.properties['SEP_counterexample'] = s
        #                 return
        #             elif check_for_extr and size == max_size:
        #                 self.properties['extr_old'] = True
        #                 # self.properties['extr_active_bound'] = s
        #                 check_for_extr = False
        #     if check_for_extr:
        #         self.properties['extr_old'] = False
        #     self.properties['subt_old'] = True

        # @timing('prop_plan')  # TODO: Not implemented!
        # def check_planarity(self):  # TODO: LEGACY ---> REMOVE
        #     self.properties['plan'] = nx.is_planar(self.graph)

        # @timing('prop_sort')  # TODO: Not implemented!
        # def check_sorted(self):  # TODO: LEGACY ---> REMOVE
        #     self.properties['sort'] = self._coding.is_sorted()

        def check_properties(self):
            self.check_subt_extr()
            # self.check_planarity()
            # self.check_sorted()
            self.check_canon()

        # GRAPHICS and FILES

        def draw(self):
            graphics.plot_graph(self.graph, self._coding, self.properties)
            graphics.save_graph_drawing(n=self.n, k=self.k, coding=self._coding)

        def save(self):
            data = {'coding': self._coding.to_save(),
                    'certificate': bytes.hex(self.certificate) if self.certificate is not None else self.certificate,
                    'GAP': self.GAP,
                    'properties': self.properties,
                    'timings': self.timings}
            utl.save_graph_file(self, data)

        def __repr__(self):
            return str(self._coding)
            # return (f"{str(self._coding):<48}    " + "  ".join(f"{k} {utl.bool_symb(v)}" for k, v in self.properties.items()))

        # def debug_update_code(self):    # TODO: REMOVE!!!!
        #     try:
        #         orig_pos = nx.planar_layout(self.graph)
        #     except nx.NetworkXException:
        #         orig_pos = nx.shell_layout(self.graph)
        #     code = self.code
        #     perms = tuple(self.original_code.code_permutations())
        #     rows = len(perms)
        #     for j, comp in enumerate(perms):
        #         # print(comp)
        #         transls = tuple(comp[0].get_translations())
        #         cols = len(transls)
        #         for i, trans in enumerate(transls):
        #             new_code = self.original_code.apply_translation(trans)
        #             self.code, self.graph = new_code, _get_graph_from_code(new_code)
        #             code = min(code, new_code)
        #             # print(f"    {new_code} - {code}")
        #             plt.subplot(rows, cols, j * cols + i + 1)
        #             pos = dict((trans[k], i) for k, i in orig_pos.items())
        #             self.draw(pos)
        #     fig = plt.gcf()
        #     fig.set_size_inches(cols * 4, rows * 4)
        #     plt.show()
        #     # fig.savefig(os.path.join(var.DATA_DIR, "temp.png"))
        #     self.code = code

    class Timings(Base):
        __tablename__ = "timings"
        coding = mapped_column(PickleType, ForeignKey(ClovenGraph.coding), primary_key=True)
        graph = relationship("ClovenGraph", back_populates="timings", lazy="immediate")
        prop_subt_extr = mapped_column(Integer, nullable=True)
        prop_canon = mapped_column(Integer, nullable=True)
        calc_certificate = mapped_column(Integer, nullable=True)
        calc_gap = mapped_column(Integer, nullable=True)

    class GAPInfo(Base):
        __tablename__ = "gap_info"
        coding = mapped_column(PickleType, ForeignKey(ClovenGraph.coding), primary_key=True)
        graph = relationship("ClovenGraph", back_populates="gap_info", lazy="immediate")
        sol_status = mapped_column(String)
        sol_term_cond = mapped_column(String)
        time_proc = mapped_column(Float)
        time_wall = mapped_column(Float)

        def __init__(self, raw, **kwargs):
            super().__init__(sol_status=raw['Status'],
                             sol_term_cond=raw['Termination condition'],
                             time_proc=raw['Time'],
                             time_wall=float(raw['Wall time']),
                             **kwargs)

    return ClovenGraph


if __name__ == '__main__':
    # G = ClovenGraph(7, (((0, 1, 2, 3), (4, 5, 6)), ((1, 5, 6), (0, 2, 3, 4))))  # SEP Y
    # G = ClovenGraph(6, (((0, 1, 2, 3), (4, 5)), ((0, 2), (1, 3), (4, 5))))  # NO SEP
    G = ClovenGraph(6, (((0, 1, 2, 3), (4, 5)), ((0, 3, 4), (1, 5, 2))))  # SEP Y, extr Y
    # G = ClovenGraph(7, (((0, 2, 4, 5), (1, 6, 3)), ((1, 2, 3, 5), (0, 4, 6))))  # SEP Y, extr N
    G.check_properties()
    print(G.properties)
    print(G.check_subt_extr())
    G.calc_gap()
    print(f"GAP is {G.GAP}")
    G.draw()
    plt.savefig("../temp.png")
