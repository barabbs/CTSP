import pynauty
from .core import Calculation
from modules import var

class CERT_Nauty(Calculation):
    CALC_TYPE = var.CALC_CERTIFICATE
    CALC_NAME = "Nauty"
    def _calc(self, graph):
        return {var.GRAPH_TABLE: {'certificate': pynauty.certificate(graph.nauty_graph)}}

CALCULATIONS_LIST = (CERT_Nauty,)