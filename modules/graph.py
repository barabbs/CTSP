from modules.coding import Coding, Cover
from modules import optimization_models as opt_mdl
import modules.utility as utl
from modules import graphics
from modules import var
import pynauty
import time
import os, logging

CANON = var.CALC_CANON
SUBT_EXTR = var.CALC_SUBT_EXTR
CERTIFICATE = var.CALC_CERTIFICATE
GAP = var.CALC_GAP


class Graph(object):
    LAZY_ATTRS = {'graph', 'nauty_graph', 'adjacency_matrix'}

    def __init__(self, n, k, weights, coding, **kwargs):
        self.n, self.k, self.weights = n, k, weights
        parts = tuple(tuple(len(c) for c in cover) for cover in coding)
        self.coding = Coding(Cover(cycles=c, weight=w) for c, w in zip(coding, self.weights))
        # self.graph, self.nauty_graph, self.adjacency_matrix = None, None, None

    # PRIVATE UTILITIES

    def _get_graph(self):
        return utl.get_graph_from_code(self.coding)

    def _get_nauty_graph(self):
        return utl.get_nauty_graph(self.n, self.k, self.graph)

    def _get_adjacency_matrix(self):
        return utl.get_adjacency_matrix(self.n, self.k, self.graph, self.weights)

    def __getattr__(self, item):
        assert item in Graph.LAZY_ATTRS
        setattr(self, item, getattr(self, f"_get_{item}")())
        return getattr(self, item)

    # CALCULATIONS

    def calc_canon(self):
        code = self.coding
        for j, comp in enumerate(self.coding.code_permutations()):
            for i, trans in enumerate(comp[0].get_translations()):
                if self.coding.apply_translation(trans) < code:
                    return {var.GRAPH_TABLE: {'prop_canon': False}}
        return {var.GRAPH_TABLE: {'prop_canon': True}}

    def calc_subt_extr(self):
        subt, extr = opt_mdl.SEP_MODEL.check_subt_extr(self.n, self.adjacency_matrix)
        return {var.GRAPH_TABLE: {'prop_subt': subt, 'prop_extr': extr}}

    def calc_cert(self):
        return {var.GRAPH_TABLE: {'certificate': pynauty.certificate(self.nauty_graph)}}

    def calc_gap(self):
        gap, raw_info = opt_mdl.GAP_MODEL.solve_instance(self.n, self.adjacency_matrix)
        return {var.GRAPH_TABLE: {'gap': gap}, var.GAP_INFO_TABLE: utl.convert_raw_gurobi_info(raw_info)}

    CALCULATIONS = {CANON: calc_canon,
                    SUBT_EXTR: calc_subt_extr,
                    CERTIFICATE: calc_cert,
                    GAP: calc_gap}

    def calculate(self, calc_type):
        calc_function = self.CALCULATIONS[calc_type]
        start = time.process_time_ns()
        result = calc_function(self)
        result.update({var.TIMINGS_TABLE: {calc_type: time.process_time_ns() - start}})
        return result

    # GRAPHICS and FILES

    def draw(self):
        graphics.plot_graph(self.graph, self.coding, self.properties)
        graphics.save_graph_drawing(n=self.n, k=self.k, coding=self.coding)

    def __repr__(self):
        return f"Graph {self.coding}"
