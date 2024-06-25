from modules.models import get_models, GRAPH, TIMINGS, GAP_INFO
from modules.graph import Graph
from modules.combinatorics import full_codings_generator
from modules import var
from modules import utility as utl

from concurrent.futures import ProcessPoolExecutor
from functools import partial
from itertools import chain
import os, logging
import time
import numpy as np

from sqlalchemy import create_engine, select, insert, update, bindparam, and_
from sqlalchemy.orm import Session, aliased
import enlighten

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
    logging.stage(f"            committing {total} results ({start} --> {end - 1})")
    commit_progbar = manager.counter(total=len(m_names), desc='Commit', leave=False)
    for mod in m_names:
        session.execute(COMMIT_TYPES[mod](models[mod]),
                        tuple(dict(id=codings[start + i], **result[mod]) for i, result in enumerate(cache)))
        commit_progbar.update()
    session.commit()
    commit_progbar.close()
    return end


def _calc_helper(calculator, n, k, weights, coding):
    # logging.stage(f"            {os.getpid():>8}({os.getpid() - os.getppid():<4})  {num:>8}  {calc_type.upper():<10} {coding}")
    graph = Graph(n=n, k=k, weights=weights, coding=coding)
    return calculator.calc(graph)
    # calc = graph.calculate(calc_type)
    # del graph
    # gc.collect()
    # return calc


def _launch_batch(batch, batch_size, codings, mapper, executor, calc_func, chunksize, batch_progbar):
    codings_batch = codings[batch * batch_size: (batch + 1) * batch_size]
    new_mapper = executor.map(calc_func, codings_batch, chunksize=chunksize)
    if len(codings_batch) > 0:
        logging.stage(f"        Batch {batch + 1:>6}")
        batch_progbar.update()
    return chain(mapper, new_mapper)


def parallel_run(engine, models, n, k, weights, calc_type, calculators, where=None, group_by=None,
                 **options):  # TODO: Sistemare questo schifo immondo.
    manager = enlighten.get_manager()
    with Session(engine) as session:
        where_smnt = tuple(getattr(models[GRAPH], attr).is_(val) for attr, val in where.items())

        if group_by is None:
            tot = session.query(models[GRAPH]).where(*where_smnt).count()
        else:
            graph_alias = aliased(models[GRAPH], name="graph_max")
            statement = select(models[GRAPH].coding).join(
                graph_alias,
                and_(getattr(models[GRAPH], group_by) == getattr(graph_alias, group_by),
                     models[GRAPH].coding > graph_alias.coding),
                isouter=True
            ).where(graph_alias.coding.is_(None), *where_smnt)
            tot = statement.count()

        if tot == 0:
            logging.trace(f"    Nothing to do :)")
            return
        chunksize = utl.calc_chunksize(n, calc_type, tot, **options)

        batch_size = chunksize * options["batch_chunks"] * options["workers"]
        batches = int(np.ceil(tot / batch_size))

        batch_progbar = manager.counter(total=batches, desc=f"Batch ", leave=False)
        result_progbar = manager.counter(total=tot, desc=f"Result", leave=False)

        logging.trace(f"    Total {tot:>8}    (chunksize {chunksize} / {batches} batches of {batch_size})")

        if group_by is None:
            statement = select(models[GRAPH].coding).where(
                *where_smnt)  # .group_by(group_by_smnt)  # .order_by(None if group_by is None else models[GRAPH].coding)
        else:
            graph_alias = aliased(models[GRAPH], name="graph_max")
            statement = select(models[GRAPH].coding).join(
                graph_alias,
                and_(getattr(models[GRAPH], group_by) == getattr(graph_alias, group_by),
                     models[GRAPH].coding > graph_alias.coding),
                isouter=True
            ).where(graph_alias.coding.is_(None), *where_smnt)
        # codings_gen = session.scalars(statement).partitions(size=batch_size)
        codings = session.scalars(statement).all()
        calculator = calculators.get_calculation(calc_type, n=n, k=k, weight=weights, **options)
        calc_func = partial(_calc_helper, calculator, n, k, weights)

        next_commit, commit_counter, cache, committed = time.time() + options["commit_interval"], 0, list(), 0
        mapper = tuple()
        with ProcessPoolExecutor(max_workers=options["workers"],
                                 initializer=os.nice, initargs=(var.PROCESSES_NICENESS,),
                                 max_tasks_per_child=chunksize * options["batch_chunks"],
                                 ) as executor:
            for pre_batch in range(options["preloaded_batches"]):
                mapper = _launch_batch(pre_batch, batch_size, codings, mapper, executor, calc_func, chunksize,
                                       batch_progbar)
            for batch in range(options["preloaded_batches"], batches + options["preloaded_batches"]):
                mapper = _launch_batch(batch, batch_size, codings, mapper, executor, calc_func, chunksize,
                                       batch_progbar)
                for i in range(batch_size):
                    try:
                        cache.append(next(mapper))
                    except StopIteration:
                        break
                    commit_counter += 1
                    result_progbar.update()
                    if time.time() > next_commit or commit_counter >= options["max_commit_cache"]:
                        committed = _commit_cached(session=session, models=models,
                                                   cache=cache, codings=codings,
                                                   start=committed, manager=manager)
                        next_commit, commit_counter, cache = time.time() + options["commit_interval"], 0, list()
            _commit_cached(session=session, models=models,
                           cache=cache, codings=codings,
                           start=committed, manager=manager)
    batch_progbar.close()
    result_progbar.close()
    manager.stop()
