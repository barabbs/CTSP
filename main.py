from modules import ctsp
from modules import var
from modules.combinatorics import CODINGS_GENERATORS
from modules.calculations import CANON, CERTIFICATE, SUBT_EXTR, GAP, CALCULATIONS
import argparse

import sys
import logging
from functools import partial, partialmethod

logging.TRACE = 18
logging.addLevelName(logging.TRACE, 'TRACE')
logging.Logger.trace = partialmethod(logging.Logger.log, logging.TRACE)
logging.trace = partial(logging.log, logging.TRACE)

logging.STAGE = 15
logging.addLevelName(logging.STAGE, 'STAGE')
logging.Logger.stage = partialmethod(logging.Logger.log, logging.STAGE)
logging.stage = partial(logging.log, logging.STAGE)

logging.PROCESS = 12
logging.addLevelName(logging.PROCESS, 'PRCSS')
logging.Logger.process = partialmethod(logging.Logger.log, logging.PROCESS)
logging.process = partial(logging.log, logging.PROCESS)


def init_logging(level):
    logging.basicConfig(
        level=level,
        # format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
        format="[%(asctime)s] %(levelname)s     %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout)


# Initialize parser
parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)

parser.add_argument("-n", type=int, nargs="+",
                    help="number of nodes n in graph  (sequence separated by whitespace)\n\n", required=True)
parser.add_argument("-k", type=int, nargs="+", default=(2,),
                    help="number of covers k in graph (sequence separated by whitespace)\n\n")
parser.add_argument("-w", "--weights", type=int, default=None,
                    help="weights of covers in graph (sequence separated by whitespace)\n\n")
STRATEGIES_ROWS = '\n'.join(f" {k:<3}| {v['name']:<12} | {v['descr']}" for k, v in ctsp.STRATEGIES.items())
STRATEGIES_TABLE = f"""

    |     name     |            description
----|--------------|{'-' * 35}
{STRATEGIES_ROWS}
    |              |
    
"""
parser.add_argument("-s", "--strategy", type=str, default=var.DEFAULT_STRATEGY,
                    help=f"selected strategy for computation, from the following (default: {var.DEFAULT_STRATEGY})" + STRATEGIES_TABLE)

GENERATORS_ROWS = '\n'.join(f" {k:<3}| {v['name']:<12} | {v['descr']}" for k, v in CODINGS_GENERATORS.items())
GENERATORS_TABLE = f"""

    |     name     |            description
----|--------------|{'-' * 35}
{GENERATORS_ROWS}
    |              |

"""
parser.add_argument("-g", "--generator", type=str, default=var.DEFAULT_GENERATOR,
                    help=f"selected generator for computation, from the following (default: {var.DEFAULT_GENERATOR})" + GENERATORS_TABLE)

for calc_type in (CANON, CERTIFICATE, SUBT_EXTR, GAP):
    CALC_ROWS = '\n'.join(f"{i:>4}. {c.CALC_NAME}" for i, c in enumerate(CALCULATIONS[calc_type]))
    parser.add_argument(f"--{calc_type}", type=int, default=0,
                        help=f"selected calculation for {calc_type.upper()}, among the following (default: 0)\n" + CALC_ROWS + "\n\n")

parser.add_argument("-d", "--delete", action="store_true",
                    help="delete and re-initialize databases\n\n")
parser.add_argument("-v", "--verbose", action='count', default=0,
                    help="increase output verbosity\n\n")
parser.add_argument("-q", "--quiet", action='count', default=0,
                    help="decrease output verbosity\n\n")
parser.add_argument("--workers", type=int, default=var.CPU_COUNT,
                    help=f"[PARALL] max number of processes employed  (default: {var.CPU_COUNT}, # CPUs in machine)\n\n")
parser.add_argument("--chunktime", type=int, default=var.CHUNKTIME,
                    help=f"[PARALL] approx seconds of calculation per chunk  (default: {var.CHUNKTIME}s)\n\n")
parser.add_argument("--max_chunksize", type=int, default=var.MAX_CHUNKSIZE,
                    help=f"[PARALL] maximum number of graphs per chunk  (default: {var.MAX_CHUNKSIZE})\n\n")
parser.add_argument("--min_chunks", type=int, default=var.MIN_CHUNKS,
                    help=f"[PARALL] min chunks per run  (default: {var.MIN_CHUNKS})\n\n")
parser.add_argument("--batch_chunks", type=int, default=var.BATCH_CHUNKS,
                    help=f"[PARALL] number of chunks per batch  (default: {var.BATCH_CHUNKS})\n\n")
parser.add_argument("--preloaded_batches", type=int, default=var.PRELOADED_BATCHES,
                    help=f"[PARALL] batches to preload  (default: {var.PRELOADED_BATCHES})\n\n")

parser.add_argument("--gurobi_verbose", action="store_true",
                    help="verbosity of integrality gap optimizer gurobi\n\n")
parser.add_argument("--gurobi_reset", type=int, default=var.GUROBI_RESET,
                    help=f"reset level of gurobi model in-between instances\n\n")
parser.add_argument("--gurobi_method", type=int, default=var.GUROBI_METHOD,
                    help=f"gurobi Method parameter\n\n")
parser.add_argument("--gurobi_presolve", type=int, default=var.GUROBI_PRESOLVE,
                    help=f"gurobi Presolve parameter\n\n")
parser.add_argument("--gurobi_pre_sparsify", type=int, default=var.GUROBI_PRE_SPARSIFY,
                    help=f"gurobi PreSparsify parameter\n\n")
parser.add_argument("--gurobi_threads", type=int, default=var.GUROBI_THREADS,
                    help=f"gurobi Threads parameter, number of threads for gurobi\n\n")

# parser.add_argument("--max_chunksize", type=int, default=var.MAX_CHUNKSIZE,
#                     help=f"<parallelization> max size of chunk  (default: {var.MAX_CHUNKSIZE})")
parser.add_argument("--commit_interval", type=int, default=var.COMMIT_INTERVAL,
                    help=f"seconds between commits to database  (default: {var.COMMIT_INTERVAL}s)\n\n")
parser.add_argument("--max_commit_cache", type=int, default=var.MAX_COMMIT_CACHE,
                    help=f"maximum commit cache size before committing to database  (default: {var.MAX_COMMIT_CACHE})\n\n")
parser.add_argument("--sql_verbose", action="store_true",
                    help="verbosity of SqlAlchemy backend\n\n")

if __name__ == '__main__':
    args = parser.parse_args()
    verbosity = args.verbose - args.quiet
    if verbosity == 0:
        init_logging(level=logging.INFO)
    elif verbosity == 1:
        init_logging(level=logging.TRACE)
    elif verbosity == 2:
        init_logging(level=logging.STAGE)
    elif verbosity >= 3:
        init_logging(level=logging.DEBUG)
    elif verbosity <= -1:
        init_logging(level=logging.WARNING)

    options = {"delete": args.delete,
               "workers": args.workers,
               "chunktime": args.chunktime,
               "max_chunksize": args.max_chunksize,
               "batch_chunks": args.batch_chunks,
               "min_chunks": args.min_chunks,
               "preloaded_batches": args.preloaded_batches,

               "gurobi_verbose": args.gurobi_verbose,
               "gurobi_reset": args.gurobi_reset,
               "gurobi_method": args.gurobi_method,
               "gurobi_presolve": args.gurobi_presolve,
               "gurobi_pre_sparsify": args.gurobi_pre_sparsify,
               "gurobi_threads": args.gurobi_threads,

               "commit_interval": args.commit_interval,
               "max_commit_cache": args.max_commit_cache,
               "sql_verbose": args.sql_verbose, }

    calcs_indices = dict((calc_type, getattr(args, calc_type, 0)) for calc_type in (CANON, CERTIFICATE, SUBT_EXTR, GAP))

    for k in args.k:
        if args.weights is not None:
            assert sum(args.weights) == k
        for n in args.n:
            ctsp.run(
                k=k, n=n, weights=args.weights,
                strategy=args.strategy.upper(), generator=args.generator.lower(), calcs_indices=calcs_indices,
                **options
            )
