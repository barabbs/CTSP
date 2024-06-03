from modules.CTSP import CTSP
import argparse

import sys
import logging
from functools import partial, partialmethod


logging.TRACE_A = 18
logging.addLevelName(logging.TRACE_A, 'TRACE_A')
logging.Logger.trace_a = partialmethod(logging.Logger.log, logging.TRACE_A)
logging.trace_a = partial(logging.log, logging.TRACE_A)


logging.TRACE_B = 15
logging.addLevelName(logging.TRACE_B, 'TRACE_B')
logging.Logger.trace_b = partialmethod(logging.Logger.log, logging.TRACE_B)
logging.trace_b = partial(logging.log, logging.TRACE_B)


logging.basicConfig(
    level=logging.TRACE_A,
    # format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
    format="[%(asctime)s] %(levelname)s\t %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout)

# Initialize parser
parser = argparse.ArgumentParser()

parser.add_argument("--start", type=int, default=6, help="start number of nodes n of graph")
parser.add_argument("--stop", type=int, help="stop number of nodes n of graph")
parser.add_argument("-v", "--verbose", action="store_true", help="increase output verbosity")

if __name__ == '__main__':
    args = parser.parse_args()
    ctsp = CTSP(n_range=range(args.start, args.stop), verbose=args.verbose)
    ctsp.run()
