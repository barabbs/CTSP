from modules.graph import Graph
from modules.calculations.gap import GAP_Gurobi, GAP_Gurobi_Bound, GAP_Gurobi_Lazy, GAP_Gurobi_Lazy_Bound, \
    GAP_Gurobi_Laziest, GAP_Gurobi_Laziest_Bound
from modules.calculations import Calculators, CANON, CERTIFICATE, SUBT_EXTR, GAP
from modules import var
import multiprocessing
import numpy as np
import os, time, json, math
import psutil
from modules.ctsp import initialize_database
from modules.models import get_models, GRAPH, TIMINGS
from sqlalchemy.orm import Session
from sqlalchemy import func, select
from main import init_logging
import enlighten
import argparse, logging

N, K, W = 11, 2, (1, 1)

GAP_calcs = {(0, 0): GAP_Gurobi,
             (0, 1): GAP_Gurobi_Bound,
             (1, 0): GAP_Gurobi_Lazy,
             (1, 1): GAP_Gurobi_Lazy_Bound,
             (2, 0): GAP_Gurobi_Laziest,
             (2, 1): GAP_Gurobi_Laziest_Bound
             }

# Initialize parser
parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)

MEMORY_ATTRS = (
    "rss",
    "vms",
    # "shared",
    # "text",
    # "lib",
    "data",
    # "dirty"
)
TIME_INTERVAL = 1


def get_process_memory_usage(pid, run_event, name):
    parent_process = psutil.Process(pid)
    my_process = psutil.Process()
    history = dict()
    for attr in MEMORY_ATTRS:
        history[attr] = list()
    run_event.wait()
    start = time.time()
    while run_event.is_set():
        last_time = time.time()
        # print(f"{last_time - start:3.2f}")
        processes = filter(lambda p: p != my_process, (parent_process, *parent_process.children(recursive=True)))
        infos = tuple(proc.memory_info() for proc in processes)

        for k, v in history.items():
            v.append(sum(getattr(i, k, 0) for i in infos) / 1073741824)
        time.sleep(TIME_INTERVAL - time.time() + last_time)
    print(f"Saving history to {name}.json...", end="\t")
    with open(f"data/ram_usage/{name}.json", 'w') as f:
        json.dump({
            "history": history
        }, f, indent=var.RUN_INFO_INDENT)
    print("Done!")

def

def run(manager, samples=1000,
        gurobi_verbose=False,
        gurobi_calcindex=(0, 0),
        gurobi_reset=0,
        gurobi_method=-1,
        gurobi_presolve=-1,
        gurobi_pre_sparsify=-1,
        gurobi_threads=None, ):
    name = f"C{gurobi_calcindex[0]}{gurobi_calcindex[1]}_R{str(gurobi_reset)[0]}_M{gurobi_method}_P{gurobi_presolve}_S{gurobi_pre_sparsify}_T{str(gurobi_threads)[0]}"
    print(
        f"calc: {gurobi_calcindex}, reset: {gurobi_reset}, method: {gurobi_method}, presolve: {gurobi_presolve}, pre_sparsify: {gurobi_pre_sparsify}, threads: {gurobi_threads}\nname: {name}\n" + "-"*64)

    with open(f"data/ram_usage/samples.json", 'r') as f:
        codings = json.load(f)
    graphs = tuple(Graph(n=N, k=K, weights=W, coding=c) for c in codings)
    run_event = multiprocessing.Event()
    cron_proc = multiprocessing.Process(target=get_process_memory_usage, args=(os.getpid(), run_event, name))
    cron_proc.start()
    time.sleep(1)
    run_event.set()
    print(f"\tINITIALIZING")
    GAP_calc = GAP_calcs[gurobi_calcindex]
    checkpoints = list()
    start = time.time()
    calculator = GAP_calc(n=N, k=K, w=W, gurobi_verbose=gurobi_verbose,
                          gurobi_reset=gurobi_reset,
                          gurobi_method=gurobi_method,
                          gurobi_presolve=gurobi_presolve,
                          gurobi_pre_sparsify=gurobi_pre_sparsify,
                          gurobi_threads=gurobi_threads,
                          )
    calculator.initialize()
    print(f"\t\ttime: {time.time() - start:>.2e}")  # Around 88.6 sec for n=11!
    checkpoints.append(time.time() - start)

    progbar = manager.counter(total=samples, desc=name, leave=False)
    print(f"\tRUNNING")
    times = list()
    for graph in graphs[:samples] if samples is not None else graphs:
        checkpoints.append(time.time() - start)
        res = calculator.calc(graph)
        times.append(next(iter(res['timings'].values())))
        progbar.update()
    progbar.close()
    print(f"\t\t{np.mean(times):>.2e} ± {np.std(times):>.2e}    {res['graphs']}")

    run_event.clear()
    calculator.close()
    print("\tCLOSED")
    cron_proc.join()

    with open(f"data/ram_usage/{name}.json", 'r') as f:
        history = json.load(f)["history"]

    with open(f"data/ram_usage/{name}.json", 'w') as f:
        json.dump({
            "history": history,
            "checkpoints": checkpoints
        }, f, indent=var.RUN_INFO_INDENT)

    return history, checkpoints


def generate(samples=1000, generator=var.DEFAULT_GENERATOR, calcs_indices=None):
    metadata, models = get_models(N, K, W)
    g_mod = models[GRAPH]
    engine = initialize_database(metadata=metadata, models=models,
                                 n=N, k=K, weights=W,
                                 strategy=var.DEFAULT_STRATEGY, generator=generator,
                                 calculators=Calculators(calcs_indices))
    with (Session(engine) as session):
        statement = select(g_mod.coding).where(g_mod.prop_subt.is_(True), g_mod.prop_extr.is_(True)).order_by(
            func.random()).limit(samples)
        codings = tuple(session.scalars(statement).all())
        print(f"Saving {len(codings)} samples...")

        with open(f"data/ram_usage/samples.json", 'w') as f:
            json.dump(codings, f)


parser.add_argument("-g", "--generate", action="store_true")
parser.add_argument("-s", "--samples", type=int, default=1000),

parser.add_argument("--gurobi_verbose", action="store_true",
                    help="verbosity of integrality gap optimizer gurobi\n\n")
parser.add_argument("--gurobi_reset", type=int, nargs="+", default=(var.GUROBI_RESET,),
                    help=f"reset level of gurobi model in-between instances\n\n")
parser.add_argument("--gurobi_method", type=int, nargs="+", default=(var.GUROBI_METHOD,),
                    help=f"gurobi Method parameter\n\n")
parser.add_argument("--gurobi_presolve", type=int, nargs="+", default=(var.GUROBI_PRESOLVE,),
                    help=f"gurobi Presolve parameter\n\n")
parser.add_argument("--gurobi_pre_sparsify", type=int, nargs="+", default=(var.GUROBI_PRE_SPARSIFY,),
                    help=f"gurobi PreSparsify parameter\n\n")
parser.add_argument("--gurobi_threads", type=int, nargs="+", default=(var.GUROBI_THREADS,),
                    help=f"gurobi Threads parameter, number of threads for gurobi\n\n")
parser.add_argument("--gurobi_calc", type=int, nargs="+", default=(1,),
                    help=f"gurobi Threads parameter, number of threads for gurobi\n\n")
parser.add_argument("--gurobi_bound", type=int, nargs="+", default=(1,),
                    help=f"gurobi Threads parameter, number of threads for gurobi\n\n")

if __name__ == '__main__':
    args = parser.parse_args()
    if args.generate:
        init_logging(level=logging.TRACE)
        calcs_indices = dict(
            (calc_type, getattr(args, calc_type, 0)) for calc_type in (CANON, CERTIFICATE, SUBT_EXTR, GAP))
        generate(samples=args.samples, calcs_indices=calcs_indices)
    else:
        init_logging(level=logging.WARNING)
        manager = enlighten.get_manager()
        for r in args.gurobi_reset:
            for m in args.gurobi_method:
                for p in args.gurobi_presolve:
                    for s in args.gurobi_pre_sparsify:
                        for t in args.gurobi_threads:
                            for c in args.gurobi_calc:
                                for b in args.gurobi_bound:
                                    run(manager=manager,
                                        samples=args.samples,
                                        gurobi_verbose=args.gurobi_verbose,
                                        gurobi_calcindex=(c, b),
                                        gurobi_reset=r if r >= 0 else None,
                                        gurobi_method=m,
                                        gurobi_presolve=p,
                                        gurobi_pre_sparsify=s,
                                        gurobi_threads=t
                                        )

        manager.stop()
