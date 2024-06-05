from modules.cloven_graph import Base, get_ClovenGraph
from modules.cloven_graph import calc_certificate, calc_gap, check_subt_extr, check_canon
from modules.combinatorics import graph_codings_generator
from modules import var

from concurrent.futures import ProcessPoolExecutor
import os, logging
from math import ceil

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session


def initialize_database(graph_class, n, k, w, delete=True, verbose=True):
    path = var.DATABASE_FILEPATH.format(k=k, n=n, weights="-".join(str(i) for i in w))
    if delete and os.path.exists(path):
        os.remove(path)
        logging.info(f"Deleted database {os.path.basename(path)}")
    engine = create_engine(f"sqlite:///{path}", echo=verbose)
    if not os.path.exists(path):
        logging.info(f"Generating database {os.path.basename(path)}")
        try:
            Base.metadata.create_all(engine)
            with Session(engine) as session:
                graphs = tuple(graph_class(coding=c) for c in graph_codings_generator(n, k))
                session.add_all(graphs)
                session.commit()
        except (Exception, KeyboardInterrupt) as e:
            logging.warning(f"Interupted - Deleting database {os.path.basename(path)}")
            os.remove(path)
            raise
    logging.info(f"Loaded database {os.path.basename(path)}")
    return engine


def _get_next_batch(graphs_gen):
    try:
        return next(graphs_gen)
    except StopIteration:
        return


def _get_batch_run(session, function, graphs, res_handler, *iterables):
    with ProcessPoolExecutor() as executor:
        for graph, result in zip(graphs, executor.map(function, *iterables)):  # TODO: select right chunksize!
            res_handler(graph, *result, session=session)
            logging.debug(f"\t{graph}")


def _get_batch_run(executor, function, graphs, attrs, ex_attrs):
    return executor.map(function,  # TODO: select right chunksize!
                        *tuple((getattr(g, attr) for g in graphs) for attr in (attrs or tuple())),
                        *tuple((getattr(g, attr)() for g in graphs) for attr in (ex_attrs or tuple())),
                        chunksize=var.CHUNKSIZE)

def _handle_batch_result(session, batch_run, graphs, res_handler):
    for graph, result in zip(graphs, batch_run):
        res_handler(graph, *result, session=session)
    logging.stage(f"\t\t\tCommitting...")
    session.commit()


def _parallel_run(session, graph_class, statement, function, res_handler, attrs=None, ex_attrs=None):
    tot = session.query(graph_class).where(*statement).count()
    if tot == 0:
        logging.trace(f"\tNothing to do :)")
        return
    batch_size = var.CPU_COUNT * var.CHUNKSIZE * var.N_CHUNKS
    i, n_batches = 1, ceil(tot / batch_size)
    logging.trace(f"\tTotal {tot:>8}\t({n_batches} batches of {batch_size} graphs)")
    graphs_gen = session.scalars(
        select(graph_class).where(*statement).execution_options(yield_per=batch_size)).partitions()
    with ProcessPoolExecutor(max_workers=var.CPU_COUNT) as executor:
        logging.stage(f"\t\t => PrLoad {i:>5}/{n_batches}")
        next_batch = _get_next_batch(graphs_gen)
        batch_run, graphs = None, None
        while next_batch is not None:
            logging.trace(f"\t\tBatch {i:>5}/{n_batches}")
            last_graphs, last_batch_run = graphs, batch_run
            graphs = next_batch
            logging.stage(f"\t\t => Launch {i:>5}/{n_batches}")
            batch_run = _get_batch_run(executor, function, graphs, attrs, ex_attrs)
            logging.stage(f"\t\t => PrLoad {i+1:>5}/{n_batches}")
            next_batch = _get_next_batch(graphs_gen)
            if next_batch is None:
                logging.stage(f"\t\t\tNothing more to load!")
            if last_batch_run is not None:
                logging.stage(f"\t\t => Handle {i-1:>5}/{n_batches}")
                _handle_batch_result(session, last_batch_run, last_graphs, res_handler)
            i += 1
        logging.trace(f"\t\tFinishing up...")
        logging.stage(f"\t\t => Handle {i-1:>5}/{n_batches}")
        _handle_batch_result(session, batch_run, graphs, res_handler)


def _subt_extr_handler(graph, timing, props, **kwargs):
    graph.set_property('subt', props[0])
    graph.set_property('extr', props[1])
    graph.set_timing('prop_subt_extr', timing)


def _canon_handler(graph, timing, prop, **kwargs):
    graph.set_property('canon', prop)
    graph.set_timing('prop_canon', timing)


def _certificate_handler(graph, timing, certificate, **kwargs):
    graph.certificate = certificate
    graph.set_timing('calc_certificate', timing)


def _gap_handler(graph, timing, gap_raw, session, **kwargs):
    graph.set_gap(*gap_raw, session=session)
    graph.set_timing('calc_gap', timing)


def _check_properties(graph_class, engine, n, *args, **kwargs):
    with Session(engine) as session:
        logging.info("Starting SUBT & EXTR check")
        _parallel_run(session=session,
                      graph_class=graph_class,
                      statement=(graph_class.prop_subt.is_(None),),
                      function=check_subt_extr,
                      res_handler=_subt_extr_handler,
                      attrs=("n",),
                      ex_attrs=("get_adjacency_matrix",))
        logging.info("Finished SUBT & EXTR check")
        logging.info("Starting CANON check")
        _parallel_run(session=session,
                      graph_class=graph_class,
                      statement=(graph_class.prop_canon.is_(None),),
                      function=check_canon,
                      res_handler=_canon_handler,
                      attrs=("_coding",))  # TODO: Do not access private attribute!
        logging.info("Finished CANON check")


def _calc_certificates(graph_class, engine, *args, **kwargs):
    with Session(engine) as session:
        logging.info("Starting CERTIFICATE calc")
        _parallel_run(session=session,
                      graph_class=graph_class,
                      statement=(graph_class.certificate.is_(None),),
                      function=calc_certificate,
                      res_handler=_certificate_handler,
                      ex_attrs=("get_nauty_graph",))
        logging.info("Finished CERTIFICATE calc")


def _calc_gaps(graph_class, engine, n, *args, **kwargs):
    with Session(engine) as session:
        logging.info("Starting GAP calc")
        _parallel_run(session=session,
                      graph_class=graph_class,
                      statement=(graph_class.prop_subt, graph_class.prop_extr, graph_class.prop_canon, graph_class.gap.is_(None)),
                      function=calc_gap,
                      res_handler=_gap_handler,
                      attrs=("n",),
                      ex_attrs=("get_adjacency_matrix",))
        logging.info("Finished GAP calc")


def run(n, k, weights, delete, sql_verbose, process_opt):
    weights = weights or (1,) * k
    graph_class = get_ClovenGraph(n, k, weights)
    engine = initialize_database(graph_class, n, k, weights, delete, sql_verbose)
    _check_properties(graph_class, engine, n, process_opt)
    _calc_certificates(graph_class, engine, n, process_opt)
    _calc_gaps(graph_class, engine, n, process_opt)