import os
from modules import var
from modules.models import get_models, GRAPH
from modules.ctsp import initialize_database
from main import STRATEGIES_TABLE, GENERATORS_TABLE
from modules.calculations import Calculators, CANON, CERTIFICATE, SUBT_EXTR, GAP, CALCULATIONS

from sqlalchemy import select
from sqlalchemy.orm import Session
import enlighten
import argparse

# Initialize parser
parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)

parser.add_argument("-n", type=int, nargs="+",
                    help="number of nodes n in graph  (sequence separated by whitespace)\n\n", required=True)
parser.add_argument("-k", type=int, nargs="+", default=(2,),
                    help="number of covers k in graph (sequence separated by whitespace)\n\n")
parser.add_argument("-w", "--weights", type=int, default=None,
                    help="weights of covers in graph (sequence separated by whitespace)\n\n")
parser.add_argument("-b", "--best", type=int, default=None,
                    help="number of graphs with highest gap to draw\n\n")
parser.add_argument("-g", "--only_gap", action="store_true",
                    help="only draw graphs with an integrality gap (implicit when -b specified)\n\n")
parser.add_argument("-s", "--strategy", type=str, default=var.DEFAULT_STRATEGY,
                    help=f"selected strategy for computation, from the following (default: {var.DEFAULT_STRATEGY})" + STRATEGIES_TABLE)
parser.add_argument("--generator", type=str, default=var.DEFAULT_GENERATOR,
                    help=f"selected generator for computation, from the following (default: {var.DEFAULT_GENERATOR})" + GENERATORS_TABLE)
for calc_type in (CANON, CERTIFICATE, SUBT_EXTR, GAP):
    CALC_ROWS = '\n'.join(f"{i:>4}. {c.CALC_NAME}" for i, c in enumerate(CALCULATIONS[calc_type]))
    parser.add_argument(f"--{calc_type}", type=int, default=0,
                        help=f"selected calculation for {calc_type.upper()}, among the following (default: 0)\n" + CALC_ROWS + "\n\n")


def run(n, k=2, weights=None, strategy=var.DEFAULT_STRATEGY, only_gap=True, n_best=None,
        generator=var.DEFAULT_GENERATOR, calculators=None):
    manager = enlighten.get_manager()
    weights = weights or (1,) * k
    metadata, models = get_models(n, k, weights)
    engine = initialize_database(metadata=metadata, models=models,
                                 n=n, k=k, weights=weights,
                                 strategy=strategy, generator=generator, calculators=calculators)
    with Session(engine) as session:
        statement = select(models[GRAPH])
        if only_gap:
            statement = statement.where(models[GRAPH].gap.is_not(None)).order_by(models[GRAPH].gap.desc())
        graphs = session.scalars(statement).all()
        tot = n_best or len(graphs)
        print(f"Drawing {tot} graphs...")
        progbar = manager.counter(total=n_best or len(graphs), desc=f"Drawing", leave=False)
        for i, graph in enumerate(graphs):
            if n_best is not None and i == n_best:
                break
            if only_gap:
                print(f"\t{str(graph):<64}    gap: {graph.gap:.5f}")
            graph.draw()
            progbar.update()
    progbar.close()
    manager.stop()


if __name__ == '__main__':
    args = parser.parse_args()
    gap = args.only_gap or args.best is not None
    calcs_indices = dict((calc_type, getattr(args, calc_type, 0)) for calc_type in (CANON, CERTIFICATE, SUBT_EXTR, GAP))
    calculators = Calculators(calcs_indices)
    for k in args.k:
        if args.weights is not None:
            assert sum(args.weights) == k
        for n in args.n:
            if n > 8 and not gap:
                ans = input(
                    f"n = {n} is very high to draw all graphs (use -b or -g).\nDo you want to proceed anyway? (y/n)  > ")
                if ans.upper() != "Y":
                    continue
            run(n=n, k=k, weights=args.weights,
                strategy=args.strategy.upper(), generator=args.generator.lower(), calculators=calculators,
                only_gap=gap, n_best=args.best)
