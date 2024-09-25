import os
from modules import var
from modules.calculations import Calculators, CANON, CERTIFICATE, SUBT_EXTR, GAP, CALCULATIONS
from modules.calculations.canon import CANON_Direct
from modules.calculations.cert import CERT_Nauty
from modules.calculations.subt_extr import SUBTEXTR_Matrix_Chain, SUBTEXTR_Matrix_Matrix
from modules.ctsp import initialize_database
from modules.models import get_models, GRAPH, TIMINGS
from sqlalchemy.orm import Session
import numpy as np
import json
import logging
import argparse
from sqlalchemy import func, select
from main import parser, init_logging
import enlighten

CALCS = {"canon": CANON_Direct,
         "cert": CERT_Nauty,
         "subt_extr": SUBTEXTR_Matrix_Chain,
         "subt_extr_mat": SUBTEXTR_Matrix_Matrix}

PROPS = ('canon', 'cert', 'subt_extr')


def generate_combs(props=PROPS, d=None):
    if len(props) == 0:
        yield d
        return
    d = d or dict()
    for i in (False, True):
        d[props[0]] = i
        yield from generate_combs(props[1:], d)


def _get_count(session, models, **kwargs):
    stats = dict()
    g_class = models[GRAPH]
    query = session.query(g_class).where(
        g_class.certificate.is_(None) | g_class.prop_canon.is_(None) | g_class.prop_subt.is_(None))
    if query.count() > 0:
        print("ERROR - There are some graphs with not properties not calculated")
        raise ValueError

    for checks in generate_combs():
        query = session.query(g_class)
        if checks["canon"]:
            query = query.where(g_class.prop_canon.is_(True))
        if checks["cert"]:
            query = query.group_by(g_class.certificate)
        if checks["subt_extr"]:
            query = query.where(g_class.prop_subt.is_(True), g_class.prop_extr.is_(True))
        index = "".join(str(int(v)) for v in checks.values())
        count = query.count()
        print(f"\t{index}  -  {count}")
        stats[index] = count
    return stats


def _get_timings(session, models, **kwargs):
    stats = dict()
    g_class, g_timings = models[GRAPH], models[TIMINGS]
    query = session.query(*tuple(func.avg(getattr(g_timings, prop)) for prop in PROPS))
                          # func.avg(g_timings.gap)
                          # ).join(g_timings.graph)
    res = query.all()[0]
    print("\n".join(f"\t{p:<12}{r}" for p, r in zip(PROPS, res)))
    stats = dict(zip(PROPS, res))
    return stats


def _get_timing_samples(session, models, n, k, weights, samples, manager, **kwargs):
    statement = select(models[GRAPH]).order_by(func.random()).limit(samples)
    graphs = session.scalars(statement).all()
    tot = len(graphs)
    print(f"\t{tot} graphs")
    stats = dict()
    for key, calc_class in CALCS.items():
        progbar = manager.counter(total=tot, desc=f"key", leave=False)
        print(f"\t{key:<16}")
        data = list()
        calculator = calc_class(n=n, k=k, weights=weights)
        for g in graphs:
            res = calculator.calc(g._graph)
            data.append(tuple(res[var.TIMINGS_TABLE].values())[0])
            progbar.update()
        progbar.close()
        stat = (np.mean(data), tuple(np.percentile(data, p) for p in range(0, 101, 5)))
        print(f"\t\t{stat[0]}")
        stats[key] = stat
    return stats


STATS_FUNTCTIONS = {"counts": _get_count,
                    "timings": _get_timings,
                    "timing_samples": _get_timing_samples,
                    }


def run(n_range, k=2, weights=None, strategy=var.DEFAULT_STRATEGY, generator=var.DEFAULT_GENERATOR, calcs_indices=None,
        samples=10000, stats_types=dict()):
    manager = enlighten.get_manager()
    weights = weights or (1,) * k
    stats = dict((i, dict()) for i, v in stats_types.items() if v is True)
    for n in n_range:
        print(f"\n\n" + "-"*32 + f"\n\tn={n}\n" + "-"*32)
        metadata, models = get_models(n, k, weights)
        gen = "m" if n == 12 else generator
        engine = initialize_database(metadata=metadata, models=models,
                                     n=n, k=k, weights=weights,
                                     strategy=strategy, generator=gen, calculators=Calculators(calcs_indices))
        with (Session(engine) as session):
            for stat_type, stat_dict in stats.items():
                print(f"\n{stat_type.upper()}")
                stat_dict[n] = STATS_FUNTCTIONS[stat_type](session=session, models=models,
                                                           n=n, k=k, weights=weights,
                                                           samples=samples, manager=manager)
            for stat_type, stat_dict in stats.items():
                filename = f"{'-'.join(str(s) for s in n_range)}_{stat_type}"
                if stat_type == "timing_samples":
                    filename += f"_{samples}"
                with open(os.path.join(var.STATS_DIR, f"{filename}.json"), 'w') as f:
                    json.dump(stat_dict, f, indent=4)
    manager.stop()


parser.add_argument("--samples", type=int, default=10000)
parser.add_argument("-c", "--counts", action="store_true")
parser.add_argument("-t", "--timings", action="store_true")
parser.add_argument("-s", "--timing_samples", action="store_true")

if __name__ == '__main__':
    init_logging(level=logging.INFO)
    args = parser.parse_args()
    calcs_indices = dict((calc_type, getattr(args, calc_type, 0)) for calc_type in (CANON, CERTIFICATE, SUBT_EXTR, GAP))
    stats_types = dict((stat_type, getattr(args, stat_type)) for stat_type in ("counts", "timings", "timing_samples"))

    args = parser.parse_args()
    run(n_range=args.n, k=args.k[0], weights=args.weights,
        strategy=args.strategy.upper(), generator=args.generator.lower(),
        calcs_indices=calcs_indices, stats_types=stats_types)
