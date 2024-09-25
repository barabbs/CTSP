import os
from modules import var
from modules.calculations import Calculators, CANON, CERTIFICATE, SUBT_EXTR, GAP, CALCULATIONS
from modules.calculations.gap import GAP_Gurobi_Lazy as GAP_calc
from modules.ctsp import initialize_database
from modules.models import get_models, GRAPH, TIMINGS
from sqlalchemy.orm import Session
import logging
from sqlalchemy import func, select
from main import parser, init_logging
import enlighten


def run(n_range, k=2, weights=None, strategy=var.DEFAULT_STRATEGY, generator=var.DEFAULT_GENERATOR, calcs_indices=None,
        samples=100):
    manager = enlighten.get_manager()
    weights = weights or (1,) * k
    for n in n_range:
        dirpath = var.SAMPLES_DIR.format(k=k, n=n)
        os.makedirs(dirpath, exist_ok=True)
        metadata, models = get_models(n, k, weights)
        g_class = models[GRAPH]
        gen = "m" if n == 12 else generator
        engine = initialize_database(metadata=metadata, models=models,
                                     n=n, k=k, weights=weights,
                                     strategy=strategy, generator=gen, calculators=Calculators(calcs_indices))
        gap_calc = GAP_calc(n=n, k=k, weights=weights)
        with (Session(engine) as session):
            statement = select(g_class).where(g_class.prop_subt.is_(True), g_class.prop_extr.is_(True)).order_by(
                func.random()).limit(samples)
            graphs = session.scalars(statement).all()
            tot = len(graphs)
            progbar = manager.counter(total=tot, desc=f"key", leave=False)
            for i, g in enumerate(graphs):
                with open(os.path.join(dirpath, f"sample_n{n:02}_{i:03}.txt"), 'w') as f:
                    gr = g._graph
                    print(gr)
                    res = gap_calc.calc(gr)
                    gap = res[var.GRAPH_TABLE]['gap']
                    print(f"\t{gap}")
                    f.write(f"{gr}\n\nn:   {n}\ngap: {gap}\n\n" +
                            "\n".join(f"{u},{v},{w}" for u, v, w in gr.edge_count_generator(weight=True)))
            progbar.update()
        progbar.close()
    manager.stop()


parser.add_argument("--samples", type=int, default=10000)

if __name__ == '__main__':
    init_logging(level=logging.INFO)
    args = parser.parse_args()
    calcs_indices = dict((calc_type, getattr(args, calc_type, 0)) for calc_type in (CANON, CERTIFICATE, SUBT_EXTR, GAP))

    args = parser.parse_args()
    run(n_range=args.n, k=args.k[0], weights=args.weights,
        strategy=args.strategy.upper(), generator=args.generator.lower(),
        calcs_indices=calcs_indices)
