import multiprocessing
import os, time, logging, json
import signal

import psutil, gc
from collections import deque

from modules import var
from modules import utility as utl
from modules.graph import Graph


class CloseProcessPool(BaseException):
    pass


class WorkerProcess(multiprocessing.Process):
    def __init__(self, number, calculator, graph_kwargs, input_queue, output_queue, wait_interval=0, debug_memory=True):
        super().__init__(name=f"worker_{number:03}", daemon=True)
        self.number = number
        self.calculator, self.graph_kwargs = calculator, graph_kwargs
        self.input_queue, self.output_queue = input_queue, output_queue
        self.wait_interval, self.debug_memory = wait_interval, debug_memory
        self.current_item = multiprocessing.Queue(maxsize=1)
        self.psutil_proc, self.ram_history = None, deque(maxlen=var.RAM_HISTORY_ENTRIES)

    def set_current_item(self, item=None):
        if item is None:
            self.current_item.get()
        else:
            self.current_item.put(item)

    def get_current_item(self):
        if not self.current_item.empty():
            return self.current_item.get()

    def start(self):
        super().start()
        self.psutil_proc = psutil.Process(self.pid)

    def run(self):
        try:
            time.sleep(self.wait_interval)
            os.nice(var.PROCESSES_NICENESS)
            logging.debug(f"                [{self.name} {os.getpid():>6}] START")
            self.calculator.initialize()

            processed_items = 0
            while True:
                codings = self.input_queue.get(block=True)
                if codings is None:
                    break
                self.set_current_item(codings)
                results = list((
                                   coding,
                                   self.calculator.calc(Graph(**self.graph_kwargs, coding=coding))
                               ) for coding in codings)
                self.output_queue.put(results)
                processed_items += len(codings)
                del codings, results
                self.set_current_item()
                if self.debug_memory:
                    utl.save_memory_summary(f"summary_{self.name}_{os.getpid()}.txt", processed_items)
        except CloseProcessPool:
            logging.warning(f"                [{self.name} {os.getpid():>6}] INTERRUPT")
        except Exception as err:
            logging.error(f"Worker encountered an error: {err}")
            try:  # TODO: Needed???
                utl.log_error(err, name=self.name, coding=codings)
            except UnboundLocalError:
                utl.log_error(err, name=self.name)
            raise
        self.calculator.close()
        logging.debug(f"                [{self.name} {os.getpid():>6}] END  ({processed_items} processed)")
        del self.calculator

    def get_infos(self):
        cpu = self.psutil_proc.cpu_percent()
        ram = getattr(self.psutil_proc.memory_info(), var.MEMORY_ATTR) / 1073741824
        self.ram_history.append(ram)
        return cpu, ram

    def save_ram_history(self):
        with open(os.path.join(var.LOGS_DIR, f"ram_{self.name}_{self.pid}.json"), 'w') as f:
            json.dump(tuple(self.ram_history), f, indent=var.RUN_INFO_INDENT)


class ProcessPool(object):
    def __init__(self, calculator, graph_kwargs, max_workers, chunksize, initial_wait, restart_wait, debug_memory,
                 **options):
        self.calculator, self.graph_kwargs = calculator, graph_kwargs
        self.max_workers, self.chunksize = max_workers, chunksize
        self.initial_wait, self.restart_wait, self.debug_memory = initial_wait, restart_wait, debug_memory
        self.input_queue, self.output_queue = multiprocessing.Queue(), multiprocessing.Queue()
        self.processes = []
        self.cache = list()
        self.psutil_proc = psutil.Process()

    def submit(self, codings):
        for i in range(0, len(codings), self.chunksize):
            self.input_queue.put(codings[i: i + self.chunksize])

    def shutdown(self, wait=True):
        while not self.input_queue.empty():
            self.input_queue.get()
        for _ in range(self.max_workers):
            self.input_queue.put(None)  # Send poison pill
        for p in self.processes:
            if p.is_alive():
                p.join()
            p.close()
        self.cache.clear()

    def start(self):
        for i in range(self.max_workers):
            process = WorkerProcess(number=i,
                                    calculator=self.calculator, graph_kwargs=self.graph_kwargs,
                                    input_queue=self.input_queue, output_queue=self.output_queue,
                                    wait_interval=i * self.initial_wait / self.max_workers,
                                    debug_memory=self.debug_memory)
            process.start()
            self.processes.append(process)

    def get_cache(self):
        cache = self.cache
        self.cache = list()
        return cache

    def _update_cache(self, manager):
        while not self.output_queue.empty():
            results = self.output_queue.get(block=False)
            self.cache += results
            manager.update_cached(len(results))

    def _check_processes(self, manager):
        for i, process in enumerate(self.processes):
            if not process.is_alive():
                unfinished = process.get_current_item()
                manager.add_log(type="PROCESS",
                                value=f"{process.name} dead [pid: {process.pid}, exitcode: {process.exitcode}" + (
                                    f", unfinished: {' - '.join(utl.seqence_to_str(c) for c in unfinished[0])}" if unfinished is not None else "") + "]")
                manager.deaths += 1
                process.save_ram_history()
                if unfinished is not None:
                    self.input_queue.put(unfinished)
                try:
                    os.kill(process.pid, signal.SIGKILL)
                    logging.warning(f"Process with pid {process.pid} killed")
                except ProcessLookupError:
                    pass
                process.close()
                del process
                process = WorkerProcess(number=i,
                                        calculator=self.calculator, graph_kwargs=self.graph_kwargs,
                                        input_queue=self.input_queue, output_queue=self.output_queue,
                                        wait_interval=self.restart_wait,
                                        debug_memory=self.debug_memory)
                process.start()
                self.processes[i] = process
                gc.collect()

    def update(self, manager):
        self._check_processes(manager)
        self._update_cache(manager)
        return len(self.cache)

    def get_infos(self):
        infos = tuple(zip(*(proc.get_infos() for proc in self.processes)))
        cumulatives = ((sum(infos[0]) + self.psutil_proc.cpu_percent()),
                       sum(getattr(p.memory_info(), var.MEMORY_ATTR) for p in
                           (self.psutil_proc, *self.psutil_proc.children(recursive=True))) / 1073741824)
        return infos, cumulatives

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()
