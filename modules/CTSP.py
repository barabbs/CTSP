from modules.cloven_graph import ClovenGraph
from modules.combinatorics import graph_codings_generator_2

from functools import partial, partialmethod
import logging



class CTSP(object):
    def __init__(self, n_range, verbose, **kwargs):
        self.n_range = n_range
        self.verbose = verbose

    def run(self):
        for n in self.n_range:
            logging.info(f"STARTING n = {n}")
            certificates = set()
            for coding in graph_codings_generator_2(n):
                graph = ClovenGraph(n=n, coding=coding)
                graph.check_properties()
                graph.calc_certificate()
                if not graph.certificate in certificates:
                    certificates.add(graph.certificate)
                    if graph.properties["subt"] and graph.properties["extr"]:
                        graph.calc_GAP()
                graph.save()
