import os
from modules import var
from modules.calculations import Calculators, CANON, CERTIFICATE, SUBT_EXTR, GAP, CALCULATIONS
from modules.ctsp import initialize_database
from modules.models import get_models, GRAPH, TIMINGS
from sqlalchemy.orm import Session
import numpy as np
import json
import logging
import argparse
from sqlalchemy import func
from main import parser, init_logging

PROPS = ('subt_extr', 'canon', 'cert')


def _get_count(checks, classes, engines):
    res = dict()
    for n, g_model in classes.items():
        metadata, model = g_model
        g_class = model[GRAPH]
        engine = engines[n]
        with (Session(engine) as session):
            query = session.query(g_class)
            if checks["sep_extr"]:
                query = query.where(g_class.prop_subt.is_(True), g_class.prop_extr.is_(True))
            if checks["canon"]:
                query = query.where(g_class.prop_canon.is_(True))
            if checks["cert"]:
                query = query.group_by(g_class.certificate)
            res[n] = query.count()
    return res


def _get_avg(checks, classes, engines, properties):
    res = dict()
    for n, g_model in classes.items():
        print(f"\t{n}")
        metadata, model = g_model
        g_class, g_timings = model[GRAPH], model[TIMINGS]
        engine = engines[n]
        with (Session(engine) as session):
            query = session.query(*tuple(getattr(g_timings, p) for p in properties)).join(g_timings.graph)
            if checks["subt_extr"]:
                query = query.where(g_class.prop_subt.is_(True), g_class.prop_extr.is_(True))
            if checks["canon"]:
                query = query.where(g_class.prop_canon.is_(True))
            if checks["cert"]:
                query = query.group_by(g_class.certificate)
            data = np.array(query.all()).transpose()
            # print(data[0], data[1], data[2])
            res[n] = dict((p, (np.mean(d), np.std(d))) for p, d in zip(properties, data))
    return res


# TIMES = ("subt+extr", "canon", "cert", "gap")


def generate_combs(props, d=None):
    if len(props) == 0:
        yield d
        return
    d = d or dict()
    for i in (False, True):
        d[props[0]] = i
        yield from generate_combs(props[1:], d)


def run(n_range, k=2, weights=None, strategy="P", generator="h", calcs_indices=None, properties=PROPS, early_stop=True):
    weights = weights or (1,) * k
    models = dict((n, get_models(n, k, weights)) for n in n_range)
    calculator = Calculators(calcs_indices or dict())
    engines = dict(
        (n, initialize_database(c[0], c[1], n, k, weights, strategy, generator, calculator)) for n, c in models.items())
    data = dict()
    for row in (dict((p, False) for p in properties),) if early_stop else generate_combs(props=properties):
        index = "".join(str(int(v)) for v in row.values())
        print(index)
        data[index] = _get_avg(row, models, engines, properties=properties)
    with open(os.path.join(var.STATS_DIR, f"{'-'.join(str(s) for s in n_range)}_{strategy}_timings.json"), 'w') as f:
        json.dump(data, f, indent=4)

parser.add_argument("--no_early_stop", action="store_true")
parser.add_argument("--properties", type=int, nargs="+", default=range(len(PROPS)))

if __name__ == '__main__':
    init_logging(level=logging.TRACE)
    args = parser.parse_args()
    calcs_indices = dict((calc_type, getattr(args, calc_type, 0)) for calc_type in (CANON, CERTIFICATE, SUBT_EXTR, GAP))

    args = parser.parse_args()
    run(n_range=args.n, k=args.k[0], weights=args.weights,
        strategy=args.strategy.upper(), generator=args.generator.lower(),
        calcs_indices=calcs_indices,
        properties=tuple(PROPS[i] for i in args.properties),
        early_stop= not args.no_early_stop
        )
