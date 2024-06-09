import os

BASE_DIR = os.getcwd()
DATA_DIR = os.path.join(BASE_DIR, "data")
GRAPH_DATA_DIR = os.path.join(DATA_DIR, "k{k}_n{n}")
GRAPH_DRAW_DIR = os.path.join(DATA_DIR, "k{k}_n{n}-draw")
DATABASE_FILEPATH = os.path.join(DATA_DIR, "graphs_k{k}_n{n}_w{weights}_s{strategy}.cgdb")
RUN_INFO_FILEPATH = os.path.join(DATA_DIR, "run_k{k}_n{n}_w{weights}_s{strategy}.json")
RUN_INFO_INDENT = 4

# Table names
GRAPH_TABLE = 'graph'
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
MIN_CHUNKS = 10
# MAX_CHUNKSIZE = 100
EST_CALC_TIME_PARAMS = {
    "subt_extr": (1e-06, 0.95),
    "canon": (1e-04, 0),
    "cert": (1.4e-04, -0.42),
    "gap": (7e-9, 2.1)
}

DEFAULT_STRATEGY = "O1"

GRAPH_FILENAME = "{coding}.graph"
GRAPH_FILE_INDENT = None

DRAWINGS_FILENAME = "{coding}.png"
DRAWINGS_DPI = 100
