import os
from modules import var
from modules.models import get_models, GRAPH
from modules.ctsp import initialize_database
from main import STRATEGIES_TABLE, GENERATORS_TABLE
from modules.calculations import Calculators, CANON, CERTIFICATE, SUBT_EXTR, GAP, CALCULATIONS
from modules.calculations.gap import GAP_Gurobi_External

from sqlalchemy import select, func
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
parser.add_argument("-g", "--only-gap", action="store_true",
                    help="only draw graphs with an integrality gap (implicit when -b specified)\n\n")
parser.add_argument("--compute-gap", action="store_true")
parser.add_argument("--generate-samples", type=str, default=None)
parser.add_argument("-r", "--random", action="store_true",
                    help="select entries randomly\n\n")
parser.add_argument("--reduced", action="store_true")
parser.add_argument("-s", "--strategy", type=str, default=var.DEFAULT_STRATEGY,
                    help=f"selected strategy for computation, from the following (default: {var.DEFAULT_STRATEGY})" + STRATEGIES_TABLE)
parser.add_argument("--generator", type=str, default=var.DEFAULT_GENERATOR,
                    help=f"selected generator for computation, from the following (default: {var.DEFAULT_GENERATOR})" + GENERATORS_TABLE)
for calc_type in (CANON, CERTIFICATE, SUBT_EXTR, GAP):
    CALC_ROWS = '\n'.join(f"{i:>4}. {c.CALC_NAME}" for i, c in enumerate(CALCULATIONS[calc_type]))
    parser.add_argument(f"--{calc_type}", type=int, default=0,
                        help=f"selected calculation for {calc_type.upper()}, among the following (default: 0)\n" + CALC_ROWS + "\n\n")
parser.add_argument("--no-draw", action="store_true",
                    help="dry run without drawing\n\n")


def run(n, k=2, weights=None, strategy=var.DEFAULT_STRATEGY, only_gap=True, n_best=None,
        generator=var.DEFAULT_GENERATOR, calculators=None, reduced=False, workers=None,
        no_draw=False, random=False, compute_gap=False, generate_samples=None):
    manager = enlighten.get_manager()
    weights = weights or (1,) * k
    if generate_samples is not None:
        dirpath = os.path.join(var.SAMPLES_DIR, generate_samples)
        os.makedirs(dirpath, exist_ok=True)
        print(f"Generated directory {dirpath}")
    metadata, models = get_models(n, k, weights)
    engine = initialize_database(metadata=metadata, models=models,
                                 n=n, k=k, weights=weights, reduced=reduced,
                                 strategy=strategy, generator=generator, calculators=calculators, workers=workers)
    with (Session(engine) as session):
        statement = select(models[GRAPH])
        if only_gap:
            statement = statement.where(models[GRAPH].gap.is_not(None))
            if not random:
                statement = statement.order_by(models[GRAPH].gap.desc())
        if random:
            statement = statement.order_by(models[GRAPH].gap.desc()).order_by(func.random())
        if n_best is not None:
            statement = statement.limit(n_best)
        graphs = session.scalars(statement).all()
        tot = n_best or len(graphs)
        print(f"Drawing {tot} graphs (only_gap: {only_gap}, n_best: {n_best}, random: {random}, no_draw: {no_draw})")
        progbar = manager.counter(total=n_best or len(graphs), desc=f"Drawing", leave=False)
        if compute_gap:
            gap_calc = GAP_Gurobi_External(n)
        for i, graph in enumerate(graphs):
            if compute_gap:
                print("\n\n" + "-"*64 + "\n\n")
            print(f"{i:>6}. {str(graph)[9:]:<64}    gap: " + (f"{graph.gap:.5f}" if graph.gap is not None else "---"))
            if compute_gap:
                gap_calc.calc(graph._graph)
            if generate_samples is not None:
                with open(os.path.join(dirpath, f"sample_n{n:02}_{i:03}.txt"), 'w') as f:
                    gr = graph._graph
                    f.write(f"{gr}\n\nn:   {n}\ngap: {graph.gap}\n\n" +
                            "\n".join(f"{u},{v},{w}" for u, v, w in gr.edge_count_generator(weight=True)))

            if not no_draw:
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
                reduced=args.reduced, compute_gap=args.compute_gap, generate_samples=args.generate_samples,
                only_gap=gap, n_best=args.best, no_draw=args.no_draw, random=args.random)
