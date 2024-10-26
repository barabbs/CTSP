import os
import argparse
from modules import var
from modules.calculations import CANON, CERTIFICATE, SUBT_EXTR, GAP, CALCULATIONS
from modules import ctsp
from modules.calculations import Calculators
from modules.ctsp import initialize_database
from modules.models import get_models, GRAPH, TIMINGS
from sqlalchemy.orm import Session
from sqlalchemy import func, update

import logging
from functools import partial, partialmethod

logging.TRACE = 18
logging.addLevelName(logging.TRACE, 'TRACE')
logging.Logger.trace = partialmethod(logging.Logger.log, logging.TRACE)
logging.trace = partial(logging.log, logging.TRACE)

parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)

parser.add_argument("-n", type=int,
                    help="number of nodes n in graph  (sequence separated by whitespace)\n\n", required=True)
parser.add_argument("-k", type=int, default=2,
                    help="number of covers k in graph (sequence separated by whitespace)\n\n")
parser.add_argument("-w", "--weights", type=int, default=None,
                    help="weights of covers in graph (sequence separated by whitespace)\n\n")
parser.add_argument("-s", "--strategy", type=str, default=var.DEFAULT_STRATEGY,
                    help=f"selected strategy for computation, from the following (default: {var.DEFAULT_STRATEGY})")
parser.add_argument("-g", "--generator", type=str, default=var.DEFAULT_GENERATOR,
                    help=f"selected generator for computation, from the following (default: {var.DEFAULT_GENERATOR})")

if __name__ == '__main__':
    args = parser.parse_args()
    k = args.k
    n = args.n
    weights = args.weights or (1,) * k
    strategy = args.strategy.upper()
    generator = args.generator.lower()
    calcs_indices = dict((calc_type, getattr(args, calc_type, 0)) for calc_type in (CANON, CERTIFICATE, SUBT_EXTR, GAP))
    calculators = Calculators(calcs_indices)
    metadata, models = get_models(n, k, weights)
    engine = initialize_database(metadata=metadata, models=models,
                                 n=n, k=k, weights=weights,
                                 strategy=strategy, generator=generator, calculators=calculators, )
    with Session(engine) as session:
        print(f"Total  {session.query(models[GRAPH]).count()}")
        print(f"Null   {session.query(models[GRAPH]).where(models[GRAPH].gap.is_(None)).count()}")
        session.execute(update(models[GRAPH]).values(gap=None))
        session.commit()
        print(f"New    {session.query(models[GRAPH]).where(models[GRAPH].gap.is_(None)).count()}")
