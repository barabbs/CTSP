import math
import signal

from modules.models import GRAPH, TIMINGS, GAP_INFO
from modules import utility as utl
from modules.process_pool import ProcessPool

import os, logging
import time
import numpy as np

from sqlalchemy import select, insert, update, bindparam, and_
from sqlalchemy.orm import Session, aliased
import enlighten

COMMIT_UPDATE = lambda c: update(c).where(c.coding == bindparam("id"))
COMMIT_INSERT = lambda c: insert(c).values(coding=bindparam("id"))

COMMIT_TYPES = {GRAPH: COMMIT_UPDATE,
                TIMINGS: COMMIT_UPDATE,
                GAP_INFO: COMMIT_INSERT}


class Manager(enlighten.Manager):
    PROGRESSBAR_FORMAT = u'     {percentage_2:3.0f}% |{bar}|' + \
                         u' {count_2:{len_total}d}+{count_1}+{count_0}/{total:d} ' + \
                         u'[{elapsed}<{eta_2}, {interval_1:.2f}s]'
    INFOBARS_FORMAT = "{type} {cumulative:5.1f} | {values} |{post}"
    LOGBAR_FORMAT = "{type:<9} | {value}  {status}"
    INFOBARS = ["CPU", "RAM"]

    def __init__(self, total, **kwargs):
        super().__init__(**kwargs)
        # self.committed, self.cached, self.loaded = None, None, None
        self.total = total
        self.loaded = self.counter(total=total, bar_format=self.PROGRESSBAR_FORMAT,
                                   unit='graphs', color='cyan', leave=False, position=3)
        self.cached = self.loaded.add_subcounter('blue', all_fields=True)
        self.committed = self.loaded.add_subcounter('white', all_fields=True)

        self.infobars = dict((info, self.status_bar(status_format=self.INFOBARS_FORMAT,
                                                    type=info, cumulative=0, values="", post="",
                                                    poisition=i + 1, leave=False)) for i, info in
                             enumerate(self.INFOBARS))
        self.curr_info = 0
        self.logbar = self.status_bar(status_format=self.LOGBAR_FORMAT, type="", value="", status="", position=4,
                                      leave=False)

    def update(self, executor):
        infos, cumulatives = executor.get_infos()
        max_vals = (os.get_terminal_size().columns - 20) // 6
        values = (
            "  ".join(
                f"{int(s):>4}" for s in infos[0][self.curr_info * max_vals:(self.curr_info + 1) * max_vals]),
            "  ".join(f"{s:>4.1f}" for s in infos[1][self.curr_info * max_vals:(self.curr_info + 1) * max_vals]),
        )
        posts = (f" page {self.curr_info + 1}",
                 f"{self.curr_info * max_vals:3}-{min(len(infos[0]), (self.curr_info + 1) * max_vals) - 1:3}")
        for infobar, cumul, val, post in zip(self.infobars.values(), cumulatives, values, posts):
            infobar.update(cumulative=cumul, values=val, post=post)
        self.curr_info = (self.curr_info + 1) % math.ceil(len(infos[0]) / max_vals)

    def add_log(self, type="", value="", status=""):
        self.logbar.update(type=type, value=value, status=status)

    def change_log_status(self, status):
        self.logbar.update(status=status)

    def update_loaded(self, num=1):
        return self.loaded.update(num)

    def update_cached(self, num=1):
        return self.cached.update_from(self.loaded, num)

    def update_committed(self, num=1):
        return self.committed.update_from(self.cached, num)

    def print_status(self):
        for infobar in self.infobars.values():
            print(infobar)

    def finished(self):
        return self.cached.count + self.committed.count == self.total

    def stop(self):
        self.loaded.close()
        for infobar in self.infobars.values():
            infobar.close()
        self.logbar.close()
        super().stop()


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
    manager.update_committed(total)
    manager.change_log_status(status="DONE")


def _launch_batch(codings_gen, executor, manager):
    try:
        codings_batch = next(codings_gen)
    except StopIteration:
        return
    executor.submit(codings_batch)
    manager.update_loaded(len(codings_batch))


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
    raise StopIteration


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

        next_commit = time.time() + options["commit_interval"]

        with ProcessPool(
                calculator=calculator, graph_kwargs=dict(n=n, k=k, weights=weights),
                max_workers=workers, chunksize=chunksize, **options
        ) as executor:
            signal.signal(signal.SIGINT, handler)
            try:
                for pre_batch in range(options["preloaded_batches"]):
                    _launch_batch(codings_gen, executor, manager)
                while True:
                    start_time = time.time()
                    manager.update(executor)
                    if manager.loaded.count <= options["preloaded_batches"] * batch_size:
                        _launch_batch(codings_gen, executor, manager)
                    cache_length = executor.update(manager)
                    if time.time() > next_commit or cache_length >= options["max_commit_cache"]:
                        _commit_cached(session=session, models=models, executor=executor, manager=manager)
                        next_commit, time.time() + options["commit_interval"]
                    if manager.finished():
                        break
                    time.sleep(max(0, 1 - (time.time() - start_time)))
            except StopIteration:
                logging.warning(f"    KeyboardInterrupt received")
            finally:
                _commit_cached(session=session, models=models, executor=executor, manager=manager)
                signal.signal(signal.SIGINT, signal.SIG_DFL)
    manager.stop()
    return True
