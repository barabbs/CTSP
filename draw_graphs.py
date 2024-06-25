import os
from modules import var
from modules.models import get_models, GRAPH
from modules.ctsp import initialize_database
from modules.graph import Graph
from main import STRATEGIES_TABLE

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
parser.add_argument("-g", "--gap", action="store_true",
                    help="only draw graphs with an integrality gap (implicit when -b specified)\n\n")
parser.add_argument("-s", "--strategy", type=str, default=var.DEFAULT_STRATEGY,
                    help=f"selected strategy for computation, from the following (default: {var.DEFAULT_STRATEGY})" + STRATEGIES_TABLE)


def run(n, k=2, weights=None, strategy=var.DEFAULT_STRATEGY, only_gap=True, n_best=None):
    manager = enlighten.get_manager()
    weights = weights or (1,) * k
    metadata, models = get_models(n, k, weights)
    engine = initialize_database(metadata=metadata, models=models,
                                 n=n, k=k, weights=weights, strategy=strategy)
    with Session(engine) as session:
        statement = select(models[GRAPH])
        if only_gap:
            statement = statement.where(models[GRAPH].gap.is_not(None))
        graphs = session.scalars(statement).all()
        progbar = manager.counter(total=n_best or len(graphs), desc=f"Drawing", leave=False)
        for i, graph in enumerate(graphs):
            if n_best is not None and i == n_best:
                break
            graph.draw()
            progbar.update()
    progbar.close()
    manager.stop()


if __name__ == '__main__':
    args = parser.parse_args()
    for k in args.k:
        if args.weights is not None:
            assert sum(args.weights) == k
        for n in args.n:
            run(n=n, k=k, weights=args.weights, strategy=args.strategy.upper(), only_gap=args.gap, n_best=args.best)
