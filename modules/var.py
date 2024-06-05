import os

BASE_DIR = os.getcwd()
DATA_DIR = os.path.join(BASE_DIR, "data")
GRAPH_DATA_DIR = os.path.join(DATA_DIR, "k{k}_n{n}")
GRAPH_DRAW_DIR = os.path.join(DATA_DIR, "k{k}_n{n}-draw")
DATABASE_FILEPATH = os.path.join(DATA_DIR, "graphs_k{k}_n{n}_w{weights}.cgdb")

CPU_COUNT = os.cpu_count()
CHUNKSIZE = 100
N_CHUNKS = 10

GRAPH_FILENAME = "{coding}.graph"
GRAPH_FILE_INDENT = None

DRAWINGS_FILENAME = "{coding}.png"
DRAWINGS_DPI = 100

