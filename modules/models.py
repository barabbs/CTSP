import pynauty
from modules.graph import Graph
from modules import var
import modules.utility as utl

from sqlalchemy.orm import DeclarativeBase, reconstructor
from sqlalchemy import Integer, LargeBinary, Boolean, Float, String, PickleType, ForeignKey
from sqlalchemy.orm import mapped_column, relationship

GRAPH = var.GRAPH_TABLE
TIMINGS = var.TIMINGS_TABLE
GAP_INFO = var.GAP_INFO_TABLE


def get_models(N, K, W):
    class Base(DeclarativeBase):
        pass

    class DatabaseGraph(Base):
        __tablename__ = var.GRAPH_TABLE

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

        def __init__(self, coding, lazy=False, **kwargs):
            # self.n, self.k, self.weights = n, k, weights
            parts = tuple(tuple(len(c) for c in cover) for cover in coding)
            super().__init__(coding=coding, parts=parts, **kwargs)
            Timings(coding=self.coding, graph=self)
            self._graph = None
            if not lazy:
                self._init_on_load()

        @reconstructor
        def _init_on_load(self):  # TODO: Transform this to lazy loading???
            self._graph = Graph(n=self.n, k=self.k, weights=self.weights, coding=self.coding)

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
            self._graph.draw()

        def __repr__(self):
            return f"Database {self._graph}"

    class Timings(Base):
        __tablename__ = var.TIMINGS_TABLE
        coding = mapped_column(PickleType, ForeignKey(DatabaseGraph.coding), primary_key=True)
        graph = relationship("DatabaseGraph", back_populates="timings", lazy="immediate")
        subt_extr = mapped_column(Integer, nullable=True)
        canon = mapped_column(Integer, nullable=True)
        cert = mapped_column(Integer, nullable=True)
        gap = mapped_column(Integer, nullable=True)

    class GAPInfo(Base):
        __tablename__ = var.GAP_INFO_TABLE
        coding = mapped_column(PickleType, ForeignKey(DatabaseGraph.coding), primary_key=True)
        graph = relationship("DatabaseGraph", back_populates="gap_info", lazy="immediate")
        sol_status = mapped_column(String)
        sol_term_cond = mapped_column(String)
        time_proc = mapped_column(Float)
        time_wall = mapped_column(Float)

        def __init__(self, raw, **kwargs):
            super().__init__(**utl.convert_raw_gurobi_info(raw), **kwargs)

    return Base.metadata, {GRAPH: DatabaseGraph,
                           TIMINGS: Timings,
                           GAP_INFO: GAPInfo}
