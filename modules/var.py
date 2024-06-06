import os

BASE_DIR = os.getcwd()
DATA_DIR = os.path.join(BASE_DIR, "data")
GRAPH_DATA_DIR = os.path.join(DATA_DIR, "k{k}_n{n}")
GRAPH_DRAW_DIR = os.path.join(DATA_DIR, "k{k}_n{n}-draw")
DATABASE_FILEPATH = os.path.join(DATA_DIR, "graphs_k{k}_n{n}_w{weights}.cgdb")
RUN_INFO_FILEPATH = os.path.join(DATA_DIR, "run_k{k}_n{n}_w{weights}_del{delete}_ext{extensive}_str{strategy_num}.json")
RUN_INFO_INDENT = 4

CPU_COUNT = os.cpu_count()
CHUNKTIME = 2
N_CHUNKS = 10
MAX_CHUNKSIZE = 100
EST_CALC_TIME_PARAMS = {
    "subt_extr": (1e-06, 0.95),
    "canon": (1e-04, 0),
    "cert": (1.4e-04, -0.42),
    "gap": (7e-9, 2.1)
}



STRATEGY_NUM = 4

GRAPH_FILENAME = "{coding}.graph"
GRAPH_FILE_INDENT = None

DRAWINGS_FILENAME = "{coding}.png"
DRAWINGS_DPI = 100
