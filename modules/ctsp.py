from modules.models import get_models, GRAPH, TIMINGS, GAP_INFO
from modules.calculations import Calculators, CANON, CERTIFICATE, SUBT_EXTR, GAP
from modules.combinatorics import CODINGS_GENERATORS
from modules.parallelization import parallel_run
from modules import var
from modules import utility as utl

import os, logging
import time

from sqlalchemy import create_engine, select, insert, update, bindparam, func
from sqlalchemy.orm import Session
import enlighten

STRATEGIES = {
    "P": {
        'name': "properties",
        'descr': "CANON + CERT + SUB_EXT",
        'sequence': (
            (CANON, {'where': {'prop_canon': None}}),
            (CERTIFICATE, {'where': {'certificate': None}}),
            (SUBT_EXTR, {'where': {'prop_subt': None}}),
        )
    },
    "K": {
        'name': "canon",
        'descr': "CANON",
        'sequence': (
            (CANON, {'where': {'prop_canon': None}}),
        )
    },
    "C": {
        'name': "certificate",
        'descr': "CERT",
        'sequence': (
            (CERTIFICATE, {'where': {'certificate': None}}),
        )
    },
    "S": {
        'name': "prop_subt",
        'descr': "SUB_EXT",
        'sequence': (
            (SUBT_EXTR, {'where': {'prop_subt': None}}),
        )
    },
    "E": {
        'name': "extensive",
        'descr': "CANON + CERT + SUB_EXT > GAP",
        'sequence': (
            (CANON, {'where': {'prop_canon': None}}),
            (CERTIFICATE, {'where': {'certificate': None}}),
            (SUBT_EXTR, {'where': {'prop_subt': None}}),
            (GAP, {'where': {'prop_subt': True,
                             'prop_extr': True,
                             'prop_canon': True,
                             'gap': None},
                   'group_by': 'certificate'}),
        )
    },
    "1": {
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
    "2": {
        'name': "optimal_2",
        'descr': "CANON > CERT > SUB_EXT > GAP",
        'sequence': (
            (CANON, {'where': {'prop_canon': None}}),
            (CERTIFICATE, {'where': {'certificate': None,
                                     'prop_canon': True}}),
            (SUBT_EXTR, {'where': {'prop_subt': None,
                                   'prop_canon': True},
                         'group_by': 'certificate'}),
            (GAP, {'where': {'prop_canon': True,
                             'prop_subt': True,
                             'prop_extr': True,
                             'gap': None},
                   'group_by': 'certificate'}),
        )
    },
    "3": {  # CANON	SEP+EXTR	CERT
        'name': "optimal_3",
        'descr': "CANON > SUB_EXT > CERT > GAP",
        'sequence': (
            (CANON, {'where': {'prop_canon': None}}),
            (SUBT_EXTR, {'where': {'prop_subt': None,
                                   'prop_canon': True}}),
            (CERTIFICATE, {'where': {'certificate': None,
                                     'prop_canon': True,
                                     'prop_subt': True,
                                     'prop_extr': True, }}),
            (GAP, {'where': {'prop_canon': True,
                             'prop_subt': True,
                             'prop_extr': True,
                             'gap': None},
                   'group_by': 'certificate'}),
        )
    },

}


def initialize_database(metadata, models, n, k, weights, strategy, generator, calculators,
                        delete=False, sql_verbose=False, **options):
    path = var.DATABASE_FILEPATH.format(k=k, n=n, weights="-".join(str(i) for i in weights),
                                        strategy=strategy, generator=generator, calculators=calculators)
    if delete and os.path.exists(path):
        os.remove(path)
        logging.info(f"Deleted database {os.path.basename(path)}")
    engine = create_engine(f"sqlite:///{path}", echo=sql_verbose)
    if not os.path.exists(path):
        logging.info(f"Generating database {os.path.basename(path)}")
        try:
            metadata.create_all(engine)
            generator_func = CODINGS_GENERATORS[generator]['func']
            codings = tuple(generator_func(n, k))
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


def run(n, k, weights, strategy, generator, calcs_indices, **options):
    weights = weights or (1,) * k
    calculators = Calculators(calcs_indices)
    options.update({"est_calc_time_params": var.EST_CALC_TIME_PARAMS})
    infos = {"host": os.uname()[1],
             "n": n, "k": k, "weights": weights,
             "strategy": strategy,
             "generator": generator,
             "calculators": str(calculators),
             "options": options}
    options_descr = '\n    '.join(f"{i + ':':<23} {v}" for i, v in options.items())
    calcs_descr = '\n    '.join(
        f'{calc_type.upper():<12}{calc.CALC_NAME}' for calc_type, calc in calculators.calcs_classes.items())
    logging.info(f"""
    
{'-' * 128}
PARAMS         n={n}, k={k}, w={weights}

STRATEGY       {strategy} - {STRATEGIES[strategy]['name']:<12}  [{STRATEGIES[strategy]['descr']}]
GENERATION     {generator} - {CODINGS_GENERATORS[generator]['name']:<12}  [{CODINGS_GENERATORS[generator]['descr']}]
CALCULATORS    {calculators}
    {calcs_descr}

OPTIONS
    {options_descr}
{'-' * 128}

""")
    start_time = time.time()
    metadata, models = get_models(n, k, weights)
    engine = initialize_database(metadata=metadata, models=models,
                                 n=n, k=k, weights=weights,
                                 strategy=strategy, generator=generator, calculators=calculators,
                                 **options)
    utl.save_run_info_file(infos, start_time=start_time, time_name="database", delete=options['delete'])
    for calc_type, statements in STRATEGIES[strategy]['sequence']:
        logging.info(
            f"Begin {calc_type.upper():<10} (where: {', '.join(f'{a}={v}' for a, v in statements['where'].items())} / group_by: {statements.get('group_by', '--- ')})")
        start_time = time.time()
        workers = options["workers"]
        new_opt = options.copy()
        new_opt.pop("workers")
        while True:
            result = parallel_run(engine=engine, models=models,
                                  n=n, k=k, weights=weights,
                                  calc_type=calc_type, calculators=calculators, workers=workers,
                                  **statements, **new_opt)
            if result is not False:
                break
            else:
                workers -= 1
                logging.info(f"Retrying with {workers} workers (out of {options["workers"]})")
        utl.save_run_info_file(infos, start_time=start_time, time_name=calc_type)
    with Session(engine) as session:
        max_gap = session.query(func.max(models[GRAPH].gap)).scalar()
        if max_gap is not None:
            if max_gap == 0:
                max_gap = utl.get_best_gap(n - 1) or 1
                logging.info(f"Max gap not improved: {max_gap}")
            else:
                logging.info(f"Max gap found: {max_gap}")
            utl.set_best_gap(n=n, gap=max_gap)
        else:
            logging.warning(f"No gap found!")

