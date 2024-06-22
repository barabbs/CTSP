from . import canon, cert, subt_extr, gap
from modules import var

CANON = var.CALC_CANON
CERTIFICATE = var.CALC_CERTIFICATE
SUBT_EXTR = var.CALC_SUBT_EXTR
GAP = var.CALC_GAP

CALCULATIONS = {CANON: canon.CALCULATIONS_LIST,
                CERTIFICATE: cert.CALCULATIONS_LIST,
                SUBT_EXTR: subt_extr.CALCULATIONS_LIST,
                GAP: gap.CALCULATIONS_LIST}


def _get_name(canon_ind, subt_extr_ind):
    return f"{canon_ind*8 + subt_extr_ind:x}"


class Calculators(object):
    def __init__(self, calcs_indices: dict):
        # self.calcs_indices = calcs_indices
        self.calcs_classes = dict(
            (c_type, c_list[calcs_indices.get(c_type, 0)]) for c_type, c_list in CALCULATIONS.items())
        self.name = _get_name(calcs_indices[CANON], calcs_indices[SUBT_EXTR])

    def get_calculation(self, calc_type, **kwargs):
        return self.calcs_classes[calc_type](**kwargs)

    def __str__(self):
        return self.name
