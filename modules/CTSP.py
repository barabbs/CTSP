from modules.models import get_ClovenGraph
from modules.models import calc_certificate, calc_gap, check_subt_extr, check_canon
from modules.combinatorics import graph_codings_generator
from modules import var
from modules import utility as utl

from concurrent.futures import ProcessPoolExecutor
import os, logging
from math import ceil

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
import enlighten


def initialize_database(metadata, graph_class, n, k, w, delete=False, verbose=False):
    path = var.DATABASE_FILEPATH.format(k=k, n=n, weights="-".join(str(i) for i in w))
    if delete and os.path.exists(path):
        os.remove(path)
        logging.info(f"Deleted database {os.path.basename(path)}")
    engine = create_engine(f"sqlite:///{path}", echo=verbose)
    if not os.path.exists(path):
        logging.info(f"Generating database {os.path.basename(path)}")
        try:
            metadata.create_all(engine)
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


def _get_batch_run(executor, function, graphs, attrs, ex_attrs, params, chunksize):
    return executor.map(function,  # TODO: select right chunksize!
                        *tuple((getattr(g, attr) for g in graphs) for attr in (attrs or tuple())),
                        *tuple((getattr(g, attr)() for g in graphs) for attr in (ex_attrs or tuple())),
                        *tuple((p for g in graphs) for p in (params or tuple())),
                        chunksize=chunksize)


def _handle_batch_result(manager, session, batch_run, graphs, res_handler):
    handle_bar = manager.counter(total=len(graphs), desc='Handle', leave=False)
    for graph, result in zip(graphs, batch_run):
        res_handler(graph, *result, session=session)
        handle_bar.update()
    logging.stage(f"            Committing...")
    session.commit()
    handle_bar.close()


def _parallel_run(n, calc_type, session, graph_class, function, res_handler, where_smnt, group_by_col=None,
                  attrs=None, ex_attrs=None, params=None, max_workers=None, chunktime=None, n_chunks=None,
                  max_chunksize=None):
    manager = enlighten.get_manager()

    q_smnt = session.query(graph_class).where(*where_smnt)
    q_smnt = q_smnt if group_by_col is None else q_smnt.group_by(group_by_col)
    tot = q_smnt.count()
    if tot == 0:
        logging.trace(f"    Nothing to do :)")
        return

    chunksize = utl.calc_chunksize(n, calc_type, max_workers, n_chunks, chunktime, max_chunksize, tot)
    batch_size = max_workers * chunksize * n_chunks
    i, n_batches = 1, ceil(tot / batch_size)
    batches_bar = manager.counter(total=n_batches, desc='Batches', leave=False)
    logging.trace(f"    Total {tot:>8}    ({n_batches} batches of {batch_size} / chunksize {chunksize})")

    statement = select(graph_class).where(*where_smnt)
    statement = statement if group_by_col is None else statement.group_by(group_by_col)
    graphs_gen = session.scalars(statement.execution_options(yield_per=batch_size)).partitions()

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        logging.stage(f"         => PrLoad {i:>5}/{n_batches}")
        next_batch = _get_next_batch(graphs_gen)
        batch_run, graphs = None, None
        while next_batch is not None:
            logging.trace(f"        Batch {i:>5}/{n_batches}")
            last_graphs, last_batch_run = graphs, batch_run
            graphs = next_batch
            logging.stage(f"         => Launch {i:>5}/{n_batches}")
            batch_run = _get_batch_run(executor, function, graphs, attrs, ex_attrs, params, chunksize)
            logging.stage(f"         => PrLoad {i + 1:>5}/{n_batches}")
            next_batch = _get_next_batch(graphs_gen)
            if next_batch is None:
                logging.stage(f"            Nothing more to load!")
            if last_batch_run is not None:
                logging.stage(f"         => Handle {i - 1:>5}/{n_batches}")
                _handle_batch_result(manager, session, last_batch_run, last_graphs, res_handler)
            batches_bar.update()
            i += 1
        logging.trace(f"        Finishing up...")
        logging.stage(f"         => Handle {i - 1:>5}/{n_batches}")
        _handle_batch_result(manager, session, batch_run, graphs, res_handler)
        batches_bar.close()
        manager.stop()


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


def _calc_certificates(n, graph_class, engine, process_opt, *args, **kwargs):
    with Session(engine) as session:
        logging.info("Starting CERTIFICATE calc")
        _parallel_run(n=n,
                      calc_type="cert",
                      session=session,
                      graph_class=graph_class,
                      where_smnt=(graph_class.certificate.is_(None),),
                      function=calc_certificate,
                      res_handler=_certificate_handler,
                      ex_attrs=("get_nauty_graph",),
                      **process_opt)
        logging.info("Finished CERTIFICATE calc")


def _check_subt_extr(n, graph_class, engine, extensive, process_opt, *args, **kwargs):
    group_by_col = graph_class.certificate if not extensive else None
    with Session(engine) as session:
        logging.info("Starting SUBT & EXTR check")
        _parallel_run(n=n,
                      calc_type="subt_extr",
                      session=session,
                      graph_class=graph_class,
                      where_smnt=(graph_class.prop_subt.is_(None),),
                      group_by_col=group_by_col,
                      function=check_subt_extr,
                      res_handler=_subt_extr_handler,
                      attrs=("n",),
                      ex_attrs=("get_adjacency_matrix",),
                      **process_opt)
        logging.info("Finished SUBT & EXTR check")


def _check_canon(n, graph_class, engine, extensive, process_opt, *args, **kwargs):
    if not extensive:
        logging.info("Skipping CANON check")
        return
    with Session(engine) as session:
        logging.info("Starting CANON check")
        _parallel_run(n=n,
                      calc_type="canon",
                      session=session,
                      graph_class=graph_class,
                      where_smnt=(graph_class.prop_canon.is_(None),),
                      function=check_canon,
                      res_handler=_canon_handler,
                      attrs=("_coding",),
                      **process_opt)  # TODO: Do not access private attribute!
        logging.info("Finished CANON check")


def _calc_gaps(n, graph_class, engine, extensive, process_opt, opt_verbose, *args, **kwargs):
    if extensive:
        where_smnt = (graph_class.prop_subt, graph_class.prop_extr, graph_class.prop_canon, graph_class.gap.is_(None))
    else:
        where_smnt = (graph_class.prop_subt.is_(True), graph_class.prop_extr.is_(True), graph_class.gap.is_(None))
    with Session(engine) as session:
        logging.info("Starting GAP calc")
        _parallel_run(n=n,
                      calc_type="gap",
                      session=session,
                      graph_class=graph_class,
                      where_smnt=where_smnt,
                      group_by_col=graph_class.certificate,
                      function=calc_gap,
                      res_handler=_gap_handler,
                      attrs=("n",),
                      ex_attrs=("get_adjacency_matrix",),
                      params=(opt_verbose,),
                      **process_opt, )
        logging.info("Finished GAP calc")


def run(n, k, weights, delete, extensive, process_opt, sql_verbose, opt_verbose):
    weights = weights or (1,) * k
    info = {"host": os.uname()[1],
            "options": {
                "n": n,
                "k": k,
                "weights": weights,
                "delete": int(delete),
                "extensive": int(extensive),
                "process_opt": process_opt,
                "sql_verbose": sql_verbose,
                "opt_verbose": opt_verbose,
                "strategy_num": var.STRATEGY_NUM,
                "est_calc_time_params": var.EST_CALC_TIME_PARAMS
            },
            "timings": dict()}
    print("\n\n" + "-" * 64 +
          f"\nn={n}, k={k}, w={weights}  |  strategy #{var.STRATEGY_NUM}  |  {'del ' if delete else ''}{'ext ' if extensive else ''}\n" +
          f"process_opt: {process_opt}\n\n" +
          "-" * 64 + "\n\n")
    info = utl.save_run_info_file(info, "start")
    metadata, models = get_ClovenGraph(n, k, weights)
    engine = initialize_database(metadata, models['cloven_graph'], n, k, weights, delete, sql_verbose)
    info = utl.save_run_info_file(info, "database")
    _calc_certificates(n=n, graph_class=models['cloven_graph'], engine=engine, extensive=extensive,
                       process_opt=process_opt)
    info = utl.save_run_info_file(info, "cert")
    _check_subt_extr(n=n, graph_class=models['cloven_graph'], engine=engine, extensive=extensive,
                     process_opt=process_opt)
    info = utl.save_run_info_file(info, "subt_extr")
    _check_canon(n=n, graph_class=models['cloven_graph'], engine=engine, extensive=extensive, process_opt=process_opt)
    info = utl.save_run_info_file(info, "canon")
    _calc_gaps(n=n, graph_class=models['cloven_graph'], engine=engine, extensive=extensive, process_opt=process_opt,
               opt_verbose=opt_verbose)
    info = utl.save_run_info_file(info, "gap")
