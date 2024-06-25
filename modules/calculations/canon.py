from .core import Calculation
from modules.coding import Coding, Cover
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
        for coding_reorder in ((coding,) if coding.covers[0].parts != coding.covers[1].parts else (coding, ~coding)):
            base_cover = coding_reorder.covers[0]
            for cover_reorder in base_cover.cover_reordering():
                for trans in Cover(cover_reorder).get_translations():
                    if coding_reorder.apply_translation(trans) < coding:
                        return False
        return True


class CANON_Quick(CANON_Smart):
    CALC_NAME = "Quick (external C library)"

    def _canon_calc(self, coding):
        raise NotImplemented


CALCULATIONS_LIST = (CANON_Smart,
                     CANON_Direct,
                     CANON_Quick)
