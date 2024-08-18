from sqlalchemy import select, insert, update, bindparam, and_
from sqlalchemy.orm import Session, aliased

from modules.models import GRAPH, TIMINGS, GAP_INFO
from modules import utility as utl
from modules.process_pool import ProcessPool, CloseProcessPool
from modules.manager import Manager

import logging
import signal
import time
import numpy as np

COMMIT_UPDATE = lambda c: update(c).where(c.coding == bindparam("id"))
COMMIT_INSERT = lambda c: insert(c).values(coding=bindparam("id"))

COMMIT_TYPES = {GRAPH: COMMIT_UPDATE,
                TIMINGS: COMMIT_UPDATE,
                GAP_INFO: COMMIT_INSERT}


def _commit_cached(session, models, executor, manager):
    cache = executor.get_cache()
    total = len(cache)
    if total == 0:
        manager.add_log(type="COMMIT", value="Nothing to commit")
        logging.stage(f"    Nothing to commit")
        return
    manager.add_log(type="COMMIT", value=f"Committing {total} results...")
    logging.stage(f"            Committing {total} results")
    for mod in cache[0][1].keys():
        session.execute(COMMIT_TYPES[mod](models[mod]),
                        tuple(dict(id=coding, **result[mod]) for coding, result in cache))
    session.commit()
    del cache
    manager.update_committed(total)
    manager.change_log_status(status="DONE")


def _launch_batch(codings_gen, executor, manager, batch_n, batches):
    if batch_n < batches:
        manager.add_log(type="LOADING", value=f"Loading batch {batch_n + 1:3}/{batches:3}...")
    try:
        codings_batch = next(codings_gen)
    except StopIteration:
        return batch_n
    executor.submit(codings_batch)
    manager.update_loaded(len(codings_batch))
    manager.change_log_status(status="DONE")
    return batch_n + 1


def _get_statement(session, models, where, group_by):
    where_smnt = tuple(getattr(models[GRAPH], attr).is_(val) for attr, val in where.items())

    if group_by is None:
        count_stmnt = session.query(models[GRAPH]).where(*where_smnt)
    else:
        graph_alias = aliased(models[GRAPH], name="graph_max")
        count_stmnt = session.query(models[GRAPH]).join(
            graph_alias,
            and_(getattr(models[GRAPH], group_by) == getattr(graph_alias, group_by),
                 models[GRAPH].coding > graph_alias.coding),
            isouter=True
        ).where(graph_alias.coding.is_(None), *where_smnt)

    tot = count_stmnt.count()
    if tot == 0:
        return tot, None

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

    return tot, statement




def handler(sig, frame):
    raise CloseProcessPool


def parallel_run(engine, models, n, k, weights, calc_type, calculators, workers, where=None, group_by=None,
                 **options):  # TODO: Sistemare questo schifo immondo.
    with Session(engine) as session:
        tot, statement = _get_statement(session, models, where, group_by)
        if tot == 0:
            logging.trace(f"    Nothing to do :)")
            return True

        manager = Manager(total=tot)

        chunksize = utl.calc_chunksize(n, calc_type, tot, workers=workers, **options)
        batch_size = chunksize * options["batch_chunks"] * workers
        batches = int(np.ceil(tot / batch_size))

        logging.trace(f"    Total {tot:>8}    (chunksize {chunksize} / {batches} batches of {batch_size})")

        codings_gen = session.scalars(statement).partitions(size=batch_size)
        calculator = calculators.get_calculation(calc_type, n=n, k=k, weights=weights, **options)

        batch_n = 0
        next_commit = time.time() + options["commit_interval"]
        return_val = True

        with ProcessPool(
                calculator=calculator, graph_kwargs=dict(n=n, k=k, weights=weights),
                max_workers=workers, chunksize=chunksize, **options
        ) as executor:
            signal.signal(signal.SIGINT, handler)
            try:
                for pre_batch in range(options["preloaded_batches"]):
                    batch_n = _launch_batch(codings_gen, executor, manager, batch_n, batches)
                while True:
                    start_time = time.time()
                    manager.update(executor)
                    if manager.get_loaded_count() <= options["preloaded_batches"] * batch_size:
                        batch_n = _launch_batch(codings_gen, executor, manager, batch_n, batches)
                    cache_length = executor.update(manager)
                    if time.time() > next_commit or cache_length >= options["max_commit_cache"]:
                        _commit_cached(session=session, models=models, executor=executor, manager=manager)
                        next_commit = time.time() + options["commit_interval"]
                    if manager.finished():
                        break
                    time.sleep(max(0, 1 - (time.time() - start_time)))
            except CloseProcessPool:
                logging.warning(f"    KeyboardInterrupt received")
                return_val = False
            finally:
                _commit_cached(session=session, models=models, executor=executor, manager=manager)
                signal.signal(signal.SIGINT, signal.SIG_DFL)
    manager.stop()
    return return_val
