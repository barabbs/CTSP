import time

from modules.models import get_models, GRAPH, TIMINGS, GAP_INFO
from modules.graph import Graph, CANON, SUBT_EXTR, CERTIFICATE, GAP
from modules.combinatorics import graph_codings_generator
from modules import var
from modules import utility as utl

from concurrent.futures import ProcessPoolExecutor
from functools import partial
import os, logging

from sqlalchemy import create_engine, select, insert, update, bindparam
from sqlalchemy.orm import Session
import enlighten

STRATEGIES = {
    "E1": {
        'name': "ext_nogap",
        'descr': "CERT + CANON + SUB_EXT",
        'sequence': (
            (CERTIFICATE, {'where': {'certificate': None}}),
            (CANON, {'where': {'prop_canon': None}}),
            (SUBT_EXTR, {'where': {'prop_subt': None}}),
        )
    },
    "E2": {
        'name': "extensive",
        'descr': "CERT + CANON + SUB_EXT > GAP",
        'sequence': (
            (CERTIFICATE, {'where': {'certificate': None}}),
            (CANON, {'where': {'prop_canon': None}}),
            (SUBT_EXTR, {'where': {'prop_subt': None}}),
            (GAP, {'where': {'prop_subt': True,
                             'prop_extr': True,
                             'prop_canon': True,
                             'gap': None},
                   'group_by': 'certificate'}),
        )
    },
    "O1": {
        'name': "optimal_1",
        'descr': "CERT > SUB_EXT > GAP",
        'sequence': (
            (CERTIFICATE, {'where': {'certificate': None}}),
            (SUBT_EXTR, {'where': {'prop_subt': None},
                         'group_by': 'certificate'}),
            (GAP, {'where': {'prop_subt': True,
                             'prop_extr': True,
                             'gap': None},
                   'group_by': 'certificate'}),
        )
    },

}


def initialize_database(metadata, graph_class, n, k, weights, strategy, delete=False, sql_verbose=False, **options):
    path = var.DATABASE_FILEPATH.format(k=k, n=n, weights="-".join(str(i) for i in weights), strategy=strategy)
    if delete and os.path.exists(path):
        os.remove(path)
        logging.info(f"Deleted database {os.path.basename(path)}")
    engine = create_engine(f"sqlite:///{path}", echo=sql_verbose)
    if not os.path.exists(path):
        logging.info(f"Generating database {os.path.basename(path)}")
        try:
            metadata.create_all(engine)
            with Session(engine) as session:

                session.execute(insert(graph_class),
                                tuple(dict(coding=c, parts=p) for c, p in graph_codings_generator(n, k)))
                session.commit()
        except (Exception, KeyboardInterrupt) as e:
            logging.warning(f"Interupted - Deleting database {os.path.basename(path)}")
            os.remove(path)
            raise
    logging.info(f"Loaded database {os.path.basename(path)}")
    return engine


COMMIT_UPDATE = lambda c: update(c).where(c.coding == bindparam("id"))
COMMIT_INSERT = lambda c: insert(c).values(coding=bindparam("id"))

COMMIT_TYPES = {GRAPH: COMMIT_UPDATE,
                TIMINGS: COMMIT_UPDATE,
                GAP_INFO: COMMIT_INSERT}


def _commit_cached(session, models, cache, codings, start, manager):
    try:
        m_names = cache[0].keys()
    except IndexError:
        logging.trace(f"    Nothing to commit")
        return start
    total = len(cache)
    end = start + total
    logging.stage(f"        committing {total} results ({start} --> {end - 1})")
    progressbar = manager.counter(total=len(m_names), desc='committing', leave=False)
    for mod in m_names:
        session.execute(COMMIT_TYPES[mod](models[mod]),
                        tuple(dict(id=codings[start + i], **result[mod]) for i, result in enumerate(cache)))
        progressbar.update()
    session.commit()
    progressbar.close()
    return end


def _calc_helper(calc_type, n, k, weights, coding, num):
    logging.stage(f"            {os.getpid() - os.getppid():<4} ({os.getpid():<8})  {num:>8}  {calc_type.upper():<10} {coding}")
    graph = Graph(n=n, k=k, weights=weights, coding=coding)
    calc = graph.calculate(calc_type)
    logging.stage(f"            {os.getpid() - os.getppid():<4} ({os.getpid():<8})  {num:>8}  ---------- {coding}")
    return calc


def _parallel_run(engine, models, n, k, weights, calc_type, where=None, group_by=None, **options):
    manager = enlighten.get_manager()
    with Session(engine) as session:
        where_smnt = tuple(getattr(models[GRAPH], attr).is_(val) for attr, val in where.items())
        group_by_smnt = None if group_by is None else getattr(models[GRAPH], group_by)
        statement = select(models[GRAPH].coding).where(*where_smnt).group_by(group_by_smnt)
        codings = tuple(*zip(*session.execute(statement).all()))
        tot = len(codings)
        if tot == 0:
            logging.trace(f"    Nothing to do :)")
            return
        chunksize = utl.calc_chunksize(n, calc_type, tot, **options)
        progressbar = manager.counter(total=tot, desc=f"{calc_type.upper():<10}", leave=False)
        logging.trace(f"    Total {tot:>8}    (chunksize {chunksize})")
        next_commit, cache, committed = time.time() + options["commit_interval"], list(), 0
        with ProcessPoolExecutor(max_workers=options["workers"],
                                 initializer=lambda: logging.stage(f"            Started worker {os.getpid() - os.getppid():<4} ({os.getpid():<8}) ...")
                                 ) as executor:
            calc_func = partial(_calc_helper, calc_type, n, k, weights)
            mapper = executor.map(calc_func, codings, range(tot), chunksize=chunksize)
            logging.stage(f"            Launched!")
            for i, result in enumerate(mapper):
                cache.append(result)
                logging.stage(f"            Received {i}")
                progressbar.update()
                if time.time() > next_commit:
                    committed = _commit_cached(session=session,
                                               models=models,
                                               cache=cache,
                                               codings=codings,
                                               start=committed,
                                               manager=manager)
                    next_commit, cache = time.time() + options["commit_interval"], list()
        _commit_cached(session=session,
                       models=models,
                       cache=cache,
                       codings=codings,
                       start=committed,
                       manager=manager)
    progressbar.close()
    manager.stop()


def run(n, k, weights, strategy, **options):
    weights = weights or (1,) * k
    options.update({"est_calc_time_params": var.EST_CALC_TIME_PARAMS})
    infos = {"host": os.uname()[1],
             "n": n, "k": k, "weights": weights,
             "strategy": strategy,
             "options": options}
    options_descr = '\n  - '.join(f"{i + ':':<23} {v}" for i, v in options.items())
    logging.info(f"""
    
{'-' * 128}
PARAMS   n={n}, k={k}, w={weights}

STRATEGY  {strategy}, {STRATEGIES[strategy]['name']:<12}  [{STRATEGIES[strategy]['descr']}]

OPTIONS
  - {options_descr}
{'-' * 128}

""")
    start_time = time.time()
    metadata, models = get_models(n, k, weights)
    engine = initialize_database(metadata=metadata, graph_class=models[GRAPH],
                                 n=n, k=k, weights=weights, strategy=strategy, **options)
    utl.save_run_info_file(infos, start_time=start_time, time_name="database", delete=options['delete'])
    for calc_type, statements in STRATEGIES[strategy]['sequence']:
        logging.info(
            f"Begin {calc_type.upper():<10} (where: {', '.join(f'{a}={v}' for a, v in statements['where'].items())} / group_by: {statements.get('group_by', '-')})")
        start_time = time.time()
        _parallel_run(engine=engine, models=models,
                      n=n, k=k, weights=weights,
                      calc_type=calc_type, **statements, **options)
        utl.save_run_info_file(infos, start_time=start_time, time_name=calc_type)
