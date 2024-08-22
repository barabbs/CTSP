import os
from modules import var
from modules.models import get_models, GRAPH
from modules.ctsp import initialize_database
from main import STRATEGIES_TABLE, GENERATORS_TABLE
from modules.calculations import Calculators, CANON, CERTIFICATE, SUBT_EXTR, GAP, CALCULATIONS
from modules.combinatorics import partition
from modules import utility as utl
from collections import Counter

from sqlalchemy import select, func, and_
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
parser.add_argument("-s", "--strategy", type=str, default=var.DEFAULT_STRATEGY,
                    help=f"selected strategy for computation, from the following (default: {var.DEFAULT_STRATEGY})" + STRATEGIES_TABLE)
parser.add_argument("--generator", type=str, default=var.DEFAULT_GENERATOR,
                    help=f"selected generator for computation, from the following (default: {var.DEFAULT_GENERATOR})" + GENERATORS_TABLE)
for calc_type in (CANON, CERTIFICATE, SUBT_EXTR, GAP):
    CALC_ROWS = '\n'.join(f"{i:>4}. {c.CALC_NAME}" for i, c in enumerate(CALCULATIONS[calc_type]))
    parser.add_argument(f"--{calc_type}", type=int, default=0,
                        help=f"selected calculation for {calc_type.upper()}, among the following (default: 0)\n" + CALC_ROWS + "\n\n")
parser.add_argument("-d", "--details", type=float, default=None)

ROUNDING = 5


def run(n, k=2, weights=None, strategy=var.DEFAULT_STRATEGY, generator=var.DEFAULT_GENERATOR, calculators=None):
    manager = enlighten.get_manager()
    weights = weights or (1,) * k
    metadata, models = get_models(n, k, weights)
    g_class = models[GRAPH]
    engine = initialize_database(metadata=metadata, models=models,
                                 n=n, k=k, weights=weights,
                                 strategy=strategy, generator=generator, calculators=calculators)
    with open(os.path.join(var.DATA_DIR, f"k{k}_n{n}.csv"), "w") as f:
        with (Session(engine) as session):
            print(f"\nn={n}    total {session.query(g_class.coding).where().count()}\n" + "-" * 128)
            gaps = sorted(
                set(round(x, ROUNDING) for x in session.scalars(select(g_class.gap).distinct()).all() if x is not None),
                reverse=True)
            threshold = 10 ** -ROUNDING
            print(" " * 16 + "   ".join(f"{gap:#5.4}" for gap in gaps) + "    GAPS      TOTAL\n")
            f.write(",," + ",".join(f"{gap:7.6}" for gap in gaps) + ",GAPS,TOTAL\n")

            partitions = tuple(partition(n, maximum=n - 2, minimum=2))
            for i_p, p_1 in enumerate(partitions):
                for p_2 in partitions[i_p:]:
                    parts = (p_1, p_2)
                    print(f"{utl.seqence_to_str(parts):<15}", end=" ")
                    f.write("".join(str(p) for p in p_1) + "," + "".join(str(p) for p in p_2))
                    for gap in gaps:
                        val = session.query(g_class.coding).where(and_(
                            g_class.parts == parts,
                            g_class.gap <= gap + threshold,
                            g_class.gap >= gap - threshold,
                        )).count()
                        print(f"{val:>5}", end="   ")
                        f.write(f",{val}")
                    no_gap = session.query(g_class.coding).where(and_(
                        g_class.parts == parts,
                        g_class.gap.is_(None)
                    )).count()
                    total = session.query(g_class.coding).where(g_class.parts == parts).count()
                    print(f"{total - no_gap:>5}{total:>11}")
                    f.write(f",{total - no_gap},{total}\n")


def details(n, k=2, weights=None, strategy=var.DEFAULT_STRATEGY, generator=var.DEFAULT_GENERATOR, calculators=None,
            limit=None):
    weights = weights or (1,) * k
    metadata, models = get_models(n, k, weights)
    g_class = models[GRAPH]
    engine = initialize_database(metadata=metadata, models=models,
                                 n=n, k=k, weights=weights,
                                 strategy=strategy, generator=generator, calculators=calculators)
    with open(os.path.join(var.DATA_DIR, f"k{k}_n{n}-detail.csv"), "w") as f:
        with (Session(engine) as session):
            graphs = session.query(g_class).where(
                g_class.gap >= limit - 10 ** -ROUNDING
            ).order_by(g_class.gap.desc()).all()
            all_parts = set()
            datas=list()
            for graph in graphs:
                parts = session.scalars(select(g_class.parts).where(g_class.certificate == graph.certificate)).all()
                all_parts.update(set(parts))
                datas.append(Counter(parts))
                print(f"{graph.gap:#5.4}   "+"  ".join(utl.seqence_to_str(p) for p in parts))
            all_parts = sorted(all_parts)
            print("-" * 128 + "\n" + " " * 8 + "  ".join(f"{utl.seqence_to_str(p):<12}" for p in all_parts) +"\n")
            f.write("," + ",".join(utl.seqence_to_str(p) for p in all_parts) + "\n")
            for graph, data in zip(graphs, datas):
                print(f"{graph.gap:#5.4}   "+"  ".join(f"{data[p]:<12}" for p in all_parts))
                f.write(f"{graph.gap:7.6}," + ",".join(f"{data[p]}" for p in all_parts) + "\n")


if __name__ == '__main__':
    args = parser.parse_args()
    calcs_indices = dict(
        (calc_type, getattr(args, calc_type, 0)) for calc_type in (CANON, CERTIFICATE, SUBT_EXTR, GAP))
    calculators = Calculators(calcs_indices)
    for k in args.k:
        if args.weights is not None:
            assert sum(args.weights) == k
        for n in args.n:
            if args.details is not None:
                details(n=n, k=k, weights=args.weights,
                strategy=args.strategy.upper(), generator=args.generator.lower(), calculators=calculators, limit=args.details)
            else:
                run(n=n, k=k, weights=args.weights,
                    strategy=args.strategy.upper(), generator=args.generator.lower(), calculators=calculators, )
