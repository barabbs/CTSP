import os, psutil

BASE_DIR = os.getcwd()
DATA_DIR = os.path.join(BASE_DIR, "data")
ERRORS_DIR = os.path.join(DATA_DIR, 'errors')
BASE_LOGS_DIR = os.path.join(DATA_DIR, 'logs')
LOGS_DIR = os.path.join(BASE_LOGS_DIR, str(os.getpid()))
try:
    os.mkdir(LOGS_DIR)
except FileExistsError:
    pass
MODELS_DIR = os.path.join(DATA_DIR, 'models')
GRAPH_DRAW_DIR = os.path.join(DATA_DIR, "k{k}_n{n}")
DATABASE_FILEPATH = os.path.join(DATA_DIR, "graphs_k{k}_n{n}_w{weights}_{generator}{calculators}{strategy}.cgdb")
RUN_INFO_FILEPATH = os.path.join(DATA_DIR, "run_k{k}_n{n}_w{weights}_{generator}{calculators}{strategy}.json")
BEST_GAPS_FILEPATH = os.path.join(DATA_DIR, "best_gaps.json")
RUN_INFO_INDENT = 4
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

# Table names
GRAPH_TABLE = 'graphs'
TIMINGS_TABLE = 'timings'
GAP_INFO_TABLE = 'gap_info'

# Calculations names
CALC_CANON = 'canon'
CALC_SUBT_EXTR = 'subt_extr'
CALC_CERTIFICATE = 'cert'
CALC_GAP = 'gap'

CPU_COUNT = os.cpu_count()
CHUNKTIME = 1
COMMIT_INTERVAL = 60
MAX_COMMIT_CACHE = 65536
MAX_CHUNKSIZE = 1000
MIN_CHUNKS = 10
BATCH_CHUNKS = 100
GAP_WORKERS_FACTOR = 2

PRELOADED_BATCHES = 1
PROCESSES_NICENESS = 1
EST_CALC_TIME_PARAMS = {
    "subt_extr": (3.2e-04, -0.0617),
    "canon": (6.23e-04, -0.129),
    "cert": (1.93e-04, -0.0884),
    "gap": (7e-9, 2.1)
}

INITIAL_WAIT = 0
RESTART_WAIT = 60
TOTAL_RAM = psutil.virtual_memory().total / 1073741824
MEMORY_ATTR = "rss"
RAM_HISTORY_ENTRIES = 100

GUROBI_RESET = 0
GUROBI_METHOD = 1
GUROBI_PRESOLVE = 1
GUROBI_PRE_SPARSIFY = 0
GUROBI_THREADS = None

DEFAULT_STRATEGY = "1"
DEFAULT_GENERATOR = "h"

GRAPH_FILENAME = "{coding}.graph"
GRAPH_FILE_INDENT = None

DRAWINGS_FILENAME = "{coding}.png"
DRAWINGS_DPI = 100
