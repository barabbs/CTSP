import matplotlib.pyplot as plt
import pynauty
from modules.coding import Coding, Cover
from modules import optimization_models as opt_mdl
import modules.utility as utl
from modules import graphics
from modules import var

from sqlalchemy.orm import DeclarativeBase, reconstructor
from sqlalchemy import Integer, LargeBinary, Boolean, Float, String, PickleType, ForeignKey
from sqlalchemy.orm import mapped_column, relationship

#### GRAPHS CALCULATIONS

# Certificate

def calc_certificate(nauty_graph):
    return utl.timing(pynauty.certificate, nauty_graph)


def _certificate_handler(graph, timing, certificate, **kwargs):
    graph.certificate = certificate
    graph.set_timing('calc_certificate', timing)


# Gap

def calc_gap(n, adj_matrix, opt_verbose):
    def calc_gap_wrap(adj_matrix):
        return opt_mdl.GAP_MODEL.solve_instance(n, adj_matrix, opt_verbose)

    return utl.timing(calc_gap_wrap, adj_matrix)


def _gap_handler(graph, timing, gap_raw, session, **kwargs):
    graph.set_gap(*gap_raw, session=session)
    graph.set_timing('calc_gap', timing)


# Subtours and Extremality

def check_subt_extr(n, adj_matrix):
    def check_subt_extr_wrap(adj_matrix):
        return opt_mdl.SEP_MODEL.check_subt_extr(n, adj_matrix)

    return utl.timing(check_subt_extr_wrap, adj_matrix)


def _subt_extr_handler(graph, timing, props, **kwargs):
    graph.set_property('subt', props[0])
    graph.set_property('extr', props[1])
    graph.set_timing('prop_subt_extr', timing)


# Canonicity

def check_canon(coding):
    def check_canon_wrap(coding):
        for j, comp in enumerate(coding.code_permutations()):
            for i, trans in enumerate(comp[0].get_translations()):
                if coding.apply_translation(trans) < coding:
                    return False
        return True

    return utl.timing(check_canon_wrap, coding)


def _canon_handler(graph, timing, prop, **kwargs):
    graph.set_property('canon', prop)
    graph.set_timing('prop_canon', timing)


#### MODELS CONSTRUCTION

def get_ClovenGraph(N, K, W):
    class Base(DeclarativeBase):
        pass

    class ClovenGraph(Base):
        __tablename__ = "graphs"

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

        # PRIVATE UTILITIES

        def _apply_translation(self, translation):
            return tuple(cover.apply_translation(translation) for cover in self._coding)

        def _edges_in_subgraph(self, s):
            return self.graph.subgraph(s).size()

        def get_nauty_graph(self):  # TODO: Rewrite with cache
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

        # GRAPHICS and FILES

        def draw(self):
            graphics.plot_graph(self.graph, self._coding, self.properties)
            graphics.save_graph_drawing(n=self.n, k=self.k, coding=self._coding)

        def __repr__(self):
            return str(self._coding)

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

    return Base.metadata, {'cloven_graph': ClovenGraph,
                           'timings': Timings,
                           'gap_info': GAPInfo}
