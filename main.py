from modules import CTSP
from modules import var
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

logging.RESULT = 12
logging.addLevelName(logging.RESULT, 'RESLT')
logging.Logger.result = partialmethod(logging.Logger.log, logging.RESULT)
logging.result = partial(logging.log, logging.RESULT)



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
                    help="number of nodes n in graph  (sequence separated by whitespace)", required=True)
parser.add_argument("-k", type=int, nargs="+", default=(2,),
                    help="number of covers k in graph (sequence separated by whitespace)")
parser.add_argument("-w", "--weights", type=int, default=None,
                    help="weights of covers in graph (sequence separated by whitespace)")
STRATEGIES_ROWS = '\n'.join(f" {k:<3}| {v['name']:<10} | {v['descr']}" for k, v in CTSP.STRATEGIES.items())
STRATEGIES_TABLE = f"""

    |    name    |            description
----|------------|{'-' * 35}
{STRATEGIES_ROWS}
    |            |
    
"""
parser.add_argument("-s", "--strategy", type=str, default=var.DEFAULT_STRATEGY,
                    help=f"selected strategy for computation, from the following (default: {var.DEFAULT_STRATEGY})" + STRATEGIES_TABLE)
parser.add_argument("-d", "--delete", action="store_true",
                    help="delete and re-initialize databases")
parser.add_argument("-v", "--verbose", action='count', default=0,
                    help="increase output verbosity")
parser.add_argument("-q", "--quiet", action='count', default=0,
                    help="decrease output verbosity")
parser.add_argument("--workers", type=int, default=var.CPU_COUNT,
                    help=f"<parallelization> max number of processes employed  (default: {var.CPU_COUNT}, # CPUs in machine)")
parser.add_argument("--chunktime", type=int, default=var.CHUNKTIME,
                    help=f"<parallelization> approx seconds of calculation per chunk  (default: {var.CHUNKTIME}s)")
parser.add_argument("--min_chunks", type=int, default=var.MIN_CHUNKS,
                    help=f"<parallelization> minimum number of chunks  (default: {var.MIN_CHUNKS})")
# parser.add_argument("--max_chunksize", type=int, default=var.MAX_CHUNKSIZE,
#                     help=f"<parallelization> max size of chunk  (default: {var.MAX_CHUNKSIZE})")
parser.add_argument("--commit_interval", type=int, default=var.COMMIT_INTERVAL,
                    help=f"seconds between commits to database  (default: {var.COMMIT_INTERVAL}s)")
parser.add_argument("--sql_verbose", action="store_true",
                    help="verbosity of SqlAlchemy backend")
parser.add_argument("--opt_verbose", action="store_true",
                    help="verbosity of integrality gap optimizer")

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
               "min_chunks": args.min_chunks,
               "commit_interval": args.commit_interval,
               "sql_verbose": args.sql_verbose,
               "opt_verbose": args.opt_verbose}

    for k in args.k:
        if args.weights is not None:
            assert sum(args.weights) == k
        for n in args.n:
            CTSP.run(k=k, n=n, weights=args.weights, strategy=args.strategy.upper(), **options)
