from .core import Calculation
from modules import var


class CANON_Base(Calculation):
    CALC_TYPE = var.CALC_CANON

    def _canon_calc(self, coding):
        raise NotImplemented

    def _calc(self, graph):
        canon = self._canon_calc(graph.coding)
        return {var.GRAPH_TABLE: {'prop_canon': canon}}


class CANON_Direct(CANON_Base):
    CALC_NAME = "Direct"

    def _canon_calc(self, coding):
        return coding.is_canon()


class CANON_Smart(CANON_Base):
    CALC_NAME = "Smart"

    def __init__(self, k, **kwargs):
        self.k = k
        assert k == 2
        super().__init__(k=k, **kwargs)

    def _canon_calc(self, coding):
        for reorder in ((coding,) if coding.covers[0].parts != coding.covers[1].parts else (coding, ~coding)):
            base_cover = coding.covers[0]
            for trans in base_cover.get_translations():
                if reorder.apply_translation(trans) < coding:
                    return False
        return True


class CANON_Quick(CANON_Smart):
    CALC_NAME = "Quick (external C library)"

    def _canon_calc(self, coding):
        raise NotImplemented


CALCULATIONS_LIST = (CANON_Direct,
                     CANON_Smart,
                     CANON_Quick)
