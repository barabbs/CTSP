from modules.models import get_models, GRAPH, TIMINGS, GAP_INFO
from modules.calculations import Calculators, CANON, CERTIFICATE, SUBT_EXTR, GAP
from modules.combinatorics import CODINGS_GENERATORS, codings_generator
from modules.parallelization import parallel_run
from modules import var
from modules import utility as utl
from modules.graph import Graph

import os, logging
import time

from sqlalchemy import create_engine, select, update, bindparam, func
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.orm import Session
import enlighten
import multiprocessing
import queue

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
        ),
        'reduced_descr': "SUB_EXT > GAP",
        'reduced_sequence': (
            (SUBT_EXTR, {'where': {'prop_subt': None}}),
            (GAP, {'where': {'prop_subt': True,
                             'prop_extr': True,
                             'gap': None}}),
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
    "-": {
        'name': "empty",
        'descr': "",
        'sequence': ()
    },

}


class GeneratorProcess(multiprocessing.Process):
    def __init__(self, number, gen_type, n, k, weights, reduced, input_queue, max_size, cert_calculator=None):
        super().__init__(name=f"gen_process_{number:03}", daemon=True)
        self.n, self.k, self.weights = n, k, weights
        self.gen_type = gen_type
        self.reduced = reduced
        self.cert_calculator = cert_calculator
        self.input_queue, self.output_queue = input_queue, multiprocessing.Queue(maxsize=max_size)

    def run(self):
        os.nice(var.PROCESSES_NICENESS)
        while True:
            try:
                parts = self.input_queue.get(block=False)
            except queue.Empty:
                break
            if not self.reduced:
                for c in codings_generator(self.n, parts, self.gen_type, self.k):
                    self.output_queue.put({"coding": c, "parts": parts})
            else:
                self.cert_calculator.initialize()
                for c in codings_generator(self.n, parts, self.gen_type, self.k):
                    cert = self.cert_calculator.calc(Graph(n=self.n, k=self.k, weights=self.weights,
                                                           coding=c))[var.GRAPH_TABLE]['certificate']
                    self.output_queue.put({"coding": c, "parts": parts, "certificate": cert})
                self.cert_calculator.close()


class Generator(object):
    def __init__(self, generator, n, k, weights, max_workers, reduced, calculator=None, chunk=10000):
        self.n, self.k, self.weights = n, k, weights
        self.generator = generator
        self.reduced = reduced
        self.max_workers = max_workers
        self.cert_calculator = calculator
        self.chunk = chunk // self.max_workers
        self.input_queue, self.output_queue = multiprocessing.Queue(maxsize=2 * chunk), multiprocessing.Queue()

    def _update_database(self, session, models, cache):
        session.execute(insert(models[GRAPH]), cache)
        # session.execute(insert(models[TIMINGS]), tuple({"coding": entry["coding"]} for entry in cache))
        session.commit()

    def _reduced_update_database(self, session, models, cache):
        session.execute(insert(models[GRAPH]).on_conflict_do_nothing(index_elements=['certificate']), cache)
        session.commit()

    UPDATE_FUNCTIONS = {False: _update_database,
                        True: _reduced_update_database}

    def generate(self, engine, models):
        partition_func, gen_type = CODINGS_GENERATORS[self.generator]['func']
        for p in partition_func(self.n, self.k):
            self.input_queue.put(p)
        processes = tuple(GeneratorProcess(i, gen_type, self.n, self.k, self.weights, self.reduced, self.input_queue,
                                           max_size=10 * self.chunk,
                                           cert_calculator=self.cert_calculator) for i in range(self.max_workers))
        for proc in processes:
            proc.start()
        manager = enlighten.get_manager()
        progbar = manager.counter(total=None, desc='Generating', leave=False)
        update_database_func = Generator.UPDATE_FUNCTIONS[self.reduced]
        with Session(engine) as session:
            while True:
                cache = list()
                for proc in processes:
                    for _ in range(self.chunk):
                        try:
                            cache.append(proc.output_queue.get(block=False))
                        except queue.Empty:
                            pass
                if len(cache) > 0:
                    update_database_func(self, session, models, cache)
                    progbar.update(incr=len(cache))
                if not any(proc.is_alive() for proc in processes):
                    break
            session.execute(insert(models[TIMINGS]).from_select(["coding", ], select(models[GRAPH].coding)))
            session.commit()
        logging.trace(
            f"    Saved {session.query(models[GRAPH]).count()} entries out of {progbar.count} generated")
        progbar.close()
        manager.stop()


def initialize_database(metadata, models, n, k, weights, strategy, generator, calculators,
                        delete=False, sql_verbose=False, reduced=False, **options):
    path = var.DATABASE_FILEPATH.format(k=k, n=n, weights="-".join(str(i) for i in weights),
                                        strategy=strategy, generator=generator, calculators=calculators,
                                        reduced='R' if reduced else '')
    if delete and os.path.exists(path):
        os.remove(path)
        logging.trace(f"    Deleted database {os.path.basename(path)}")
    engine = create_engine(f"sqlite:///{path}", echo=sql_verbose)
    if not os.path.exists(path):
        logging.trace(f"    Generating database {os.path.basename(path)}")
        try:
            metadata.create_all(engine)
            calculator = calculators.get_calculation(CERTIFICATE, n=n, k=k, weights=weights,
                                                     **options) if reduced else None
            helper = Generator(generator, n, k, weights, max_workers=options["workers"],
                               reduced=reduced, calculator=calculator)

            helper.generate(engine, models)
        except (Exception, KeyboardInterrupt) as e:
            logging.warning(f"Interupted - Deleting database {os.path.basename(path)}")
            os.remove(path)
            raise
    logging.trace(f"    Loaded database {os.path.basename(path)}")
    return engine


def run(n, k, weights, strategy, generator, calcs_indices, reduced, **options):
    weights = weights or (1,) * k
    calculators = Calculators(calcs_indices)
    options.update({"est_calc_time_params": var.EST_CALC_TIME_PARAMS})
    infos = {"host": os.uname()[1],
             "n": n, "k": k, "weights": weights,
             "strategy": strategy,
             "generator": generator,
             "calculators": str(calculators),
             "reduced": reduced,
             "options": options}
    options_descr = '\n    '.join(f"{i + ':':<23} {v}" for i, v in options.items())
    calcs_descr = '\n    '.join(
        f'{calc_type.upper():<12}{calc.CALC_NAME}' for calc_type, calc in calculators.calcs_classes.items())
    strategy_descr = STRATEGIES[strategy].get('descr' if not reduced else 'reduced_descr') or STRATEGIES[strategy][
        'descr']
    logging.info(f"""
    
{'-' * 128}
PARAMS         n={n}, k={k}, w={weights}

STRATEGY       {strategy} - {STRATEGIES[strategy]['name']:<12}  [{strategy_descr}]    {'REDUCED' if reduced and 'reduced_descr' in STRATEGIES[strategy] else ''}
GENERATION     {generator} - {CODINGS_GENERATORS[generator]['name']:<12}  [{CODINGS_GENERATORS[generator]['descr']}]    {'REDUCED' if reduced else ''}
CALCULATORS    {calculators}
    {calcs_descr}

OPTIONS
    {options_descr}
{'-' * 128}

""")
    start_time = time.time()
    metadata, models = get_models(n, k, weights, reduced=reduced)
    logging.info("DATABASE")
    engine = initialize_database(metadata=metadata, models=models,
                                 n=n, k=k, weights=weights,
                                 strategy=strategy, generator=generator, calculators=calculators,
                                 reduced=reduced, **options)
    utl.save_run_info_file(infos, start_time=start_time, time_name="database", delete=options['delete'])
    sequence = STRATEGIES[strategy].get('sequence' if not reduced else 'reduced_sequence')
    if sequence is None:
        logging.warning("NO REDUCED SEQUENCE FOUND FOR STRATEGY - reverting to default sequence")
        sequence = STRATEGIES[strategy]['sequence']
    for calc_type, statements in sequence:
        logging.info(
            f"{calc_type.upper():<10} (where: {', '.join(f'{a}={v}' for a, v in statements['where'].items())} / group_by: {statements.get('group_by', '--- ')})")
        start_time = time.time()
        result = parallel_run(engine=engine, models=models,
                              n=n, k=k, weights=weights,
                              calc_type=calc_type, calculators=calculators,
                              **statements, **options)
        utl.save_run_info_file(infos, start_time=start_time, time_name=calc_type)
        if not result:
            break
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
