from modules.models import get_models, GRAPH, TIMINGS, GAP_INFO
from modules.graph import Graph
from modules.combinatorics import full_codings_generator
from modules import var
from modules import utility as utl

from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool, _ExceptionWithTraceback, _sendback_result
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


def _process_worker(calculator, n, k, weights, call_queue, result_queue, max_tasks=None):
    """Evaluates calls from call_queue and places the results in result_queue.

    This worker is run in a separate process.

    Args:
        call_queue: A ctx.Queue of _CallItems that will be read and
            evaluated by the worker.
        result_queue: A ctx.Queue of _ResultItems that will be written
            to by the worker.
    """
    logging.process(f"                [{os.getpid() - os.getppid():>4}] START")
    # Initialization
    os.nice(var.PROCESSES_NICENESS)
    calculator.initialize()

    num_tasks = 0
    processed_items = 0
    exit_pid = None
    while True:
        call_item = call_queue.get(block=True)
        if call_item is None:
            # Wake up queue management thread
            result_queue.put(os.getpid())
            logging.process(f"                [{os.getpid() - os.getppid():>4}] END   ({processed_items} processed)")
            return

        if max_tasks is not None:
            num_tasks += 1
            if num_tasks >= max_tasks:
                exit_pid = os.getpid()

        try:
            r = list()
            for coding in call_item.fn(*call_item.args, **call_item.kwargs):  # TODO: Remove redundant call to 'returner' func
                # print(f"{os.getpid() - os.getppid():<4} - {coding}")
                graph = Graph(n=n, k=k, weights=weights, coding=coding)
                r.append(calculator.calc(graph))
                del graph
                processed_items += 1
        except BaseException as e:
            exc = _ExceptionWithTraceback(e, e.__traceback__)
            _sendback_result(result_queue, call_item.work_id, exception=exc,
                             exit_pid=exit_pid)
        else:
            _sendback_result(result_queue, call_item.work_id, result=r,
                             exit_pid=exit_pid)
            del r

        # Liberate the resource as soon as possible, to avoid holding onto
        # open files or shared memory that is not needed anymore
        del call_item

        if exit_pid is not None:
            logging.process(f"                [{os.getpid() - os.getppid():>4}] EXIT  ({processed_items} processed)")
            return


class CalculatorProcessPool(ProcessPoolExecutor):
    def __init__(self, calculator, n, k, weights, **kwargs):
        self.calculator, self.n, self.k, self.weights = calculator, n, k, weights
        super().__init__(**kwargs)

    def _spawn_process(self):
        process_worker = partial(_process_worker, self.calculator, self.n, self.k, self.weights)
        p = self._mp_context.Process(
            target=process_worker,
            args=(self._call_queue,
                  self._result_queue,
                  self._max_tasks_per_child))
        p.start()
        self._processes[p.pid] = p


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


# def _calc_helper(calculator, n, k, weights, coding):
#     # logging.stage(f"            {os.getpid():>8}({os.getpid() - os.getppid():<4})  {num:>8}  {calc_type.upper():<10} {coding}")
#     print(f"{os.getpid() - os.getppid():<4} - {coding}")
#     # calculator.initialize()
#     graph = Graph(n=n, k=k, weights=weights, coding=coding)
#     return calculator.calc(graph)
#     # calc = graph.calculate(calc_type)
#     # del graph
#     # gc.collect()
#     # return calc

def returner(x):  # TODO: Remove redundant 'returner' func
    return x

def _launch_batch(batch, batch_size, codings, mapper, executor, chunksize, batch_progbar):
    codings_batch = codings[batch * batch_size: (batch + 1) * batch_size]
    new_mapper = executor.map(returner, codings_batch, chunksize=chunksize)  # TODO: Remove redundant call to 'returner' func
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
            tot = session.query(models[GRAPH]).join(
                graph_alias,
                and_(getattr(models[GRAPH], group_by) == getattr(graph_alias, group_by),
                     models[GRAPH].coding > graph_alias.coding),
                isouter=True
            ).where(graph_alias.coding.is_(None), *where_smnt).count()

        if tot == 0:
            logging.trace(f"    Nothing to do :)")
            return True
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
        calculator = calculators.get_calculation(calc_type, n=n, k=k, weights=weights, **options)
        # calc_func = partial(_calc_helper, calculator, n, k, weights)

        next_commit, commit_counter, cache, committed = time.time() + options["commit_interval"], 0, list(), 0
        mapper = tuple()
        with CalculatorProcessPool(
                calculator=calculator, n=n, k=k, weights=weights,
                max_workers=options["workers"],
                # max_tasks_per_child=chunksize * options["batch_chunks"],  # TODO: see if 'max_tasks_per_child' needed
                # initializer=os.nice, initargs=(var.PROCESSES_NICENESS,),
        ) as executor:
            for pre_batch in range(options["preloaded_batches"]):
                mapper = _launch_batch(pre_batch, batch_size, codings, mapper, executor, chunksize,
                                       batch_progbar)
            for batch in range(options["preloaded_batches"], batches + options["preloaded_batches"]):
                mapper = _launch_batch(batch, batch_size, codings, mapper, executor, chunksize,
                                       batch_progbar)
                for i in range(batch_size):
                    try:
                        cache.append(next(mapper))
                    except StopIteration:
                        break
                    except BrokenProcessPool:
                        logging.warning(
                            f"    (batch: {batch}, num: {i})  A process in the process pool was terminated abruptly, trying to restart...")
                        _commit_cached(session=session, models=models,
                                       cache=cache, codings=codings,
                                       start=committed, manager=manager)
                        batch_progbar.close()
                        result_progbar.close()
                        return False
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
    return True
