from modules.models import get_models, GRAPH, TIMINGS, GAP_INFO
from modules.graph import Graph, CANON, SUBT_EXTR, CERTIFICATE, GAP
from modules.combinatorics import graph_codings_generator
from modules.parallelization import parallel_run
from modules import var
from modules import utility as utl

from concurrent.futures import ProcessPoolExecutor
from functools import partial
from functools import partial
import os, logging
import time
import numpy as np

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


def initialize_database(metadata, models, n, k, weights, strategy, delete=False, sql_verbose=False, **options):
    path = var.DATABASE_FILEPATH.format(k=k, n=n, weights="-".join(str(i) for i in weights), strategy=strategy)
    if delete and os.path.exists(path):
        os.remove(path)
        logging.info(f"Deleted database {os.path.basename(path)}")
    engine = create_engine(f"sqlite:///{path}", echo=sql_verbose)
    if not os.path.exists(path):
        logging.info(f"Generating database {os.path.basename(path)}")
        try:
            metadata.create_all(engine)
            codings = tuple(graph_codings_generator(n, k))
            with Session(engine) as session:
                session.execute(insert(models[GRAPH]), tuple(dict(coding=c, parts=p) for c, p in codings))
                session.execute(insert(models[TIMINGS]), tuple(dict(coding=c) for c, p in codings))
                session.commit()
        except (Exception, KeyboardInterrupt) as e:
            logging.warning(f"Interupted - Deleting database {os.path.basename(path)}")
            os.remove(path)
            raise
    logging.info(f"Loaded database {os.path.basename(path)}")
    return engine


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
    engine = initialize_database(metadata=metadata, models=models,
                                 n=n, k=k, weights=weights, strategy=strategy, **options)
    utl.save_run_info_file(infos, start_time=start_time, time_name="database", delete=options['delete'])
    for calc_type, statements in STRATEGIES[strategy]['sequence']:
        logging.info(
            f"Begin {calc_type.upper():<10} (where: {', '.join(f'{a}={v}' for a, v in statements['where'].items())} / group_by: {statements.get('group_by', '--- ')})")
        start_time = time.time()
        parallel_run(engine=engine, models=models,
                     n=n, k=k, weights=weights,
                     calc_type=calc_type, **statements, **options)
        utl.save_run_info_file(infos, start_time=start_time, time_name=calc_type)
