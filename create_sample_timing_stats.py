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


def run(n_range, k=2, weights=None, strategy=var.DEFAULT_STRATEGY, generator=var.DEFAULT_GENERATOR, calcs_indices=None,
        samples=100):
    manager = enlighten.get_manager()
    weights = weights or (1,) * k
    stats = dict()
    for n in n_range:
        metadata, models = get_models(n, k, weights)
        engine = initialize_database(metadata=metadata, models=models,
                                     n=n, k=k, weights=weights,
                                     strategy=strategy, generator=generator, calculators=Calculators(calcs_indices))
        stats[n] = dict()
        with (Session(engine) as session):
            statement = select(models[GRAPH]).order_by(models[GRAPH].gap.desc()).order_by(func.random()).limit(samples)
            graphs = session.scalars(statement).all()
            tot = len(graphs)
            print(f"{n:>2}    {tot} graphs")
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
                stats[n][key] = stat
                with open(os.path.join(var.STATS_DIR, f"{'-'.join(str(s) for s in n_range)}_sample_timings_{samples}.json"),
                          'w') as f:
                    json.dump(stats, f, indent=4)
    manager.stop()




parser.add_argument("--samples", type=int, default=1000)

if __name__ == '__main__':
    init_logging(level=logging.TRACE)
    args = parser.parse_args()
    calcs_indices = dict(
        (calc_type, getattr(args, calc_type, 0)) for calc_type in (CANON, CERTIFICATE, SUBT_EXTR, GAP))

    args = parser.parse_args()
    run(n_range=args.n, k=args.k[0], weights=args.weights,
        strategy=args.strategy.upper(), generator=args.generator.lower(),
        calcs_indices=calcs_indices, samples=args.samples
        )
