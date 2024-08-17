import multiprocessing
import os, time, logging
import psutil

from modules import var
from modules import utility as utl
from modules.graph import Graph


class WorkerProcess(multiprocessing.Process):
    def __init__(self, number, calculator, graph_kwargs, input_queue, output_queue):
        super().__init__(name=f"worker_{number:03}", daemon=True)
        self.calculator, self.graph_kwargs = calculator, graph_kwargs
        self.input_queue, self.output_queue = input_queue, output_queue
        self.current_item = multiprocessing.Queue(maxsize=1)
        self.psutil_proc = None

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
        logging.process(f"                [{self.name} {os.getpid():>6}] START")
        os.nice(var.PROCESSES_NICENESS)
        self.calculator.initialize()

        processed_items = 0
        while True:
            try:
                codings = self.input_queue.get(block=True)
                if codings is None:
                    break
                self.set_current_item(codings)
                # if self.name == "worker_000":
                #     print(os.getpid())
                #     time.sleep(5)
                #     return
                results = list((
                                   coding,
                                   self.calculator.calc(Graph(**self.graph_kwargs, coding=coding))
                               ) for coding in codings)
                self.output_queue.put(results)

                processed_items += len(codings)
            except Exception as err:
                logging.error(f"Worker encountered an error: {err}")
                utl.log_error(err, name=self.name, coding=codings)
                raise
            else:
                del codings, results
                self.set_current_item()
        self.calculator.close()
        logging.process(f"                [{self.name} {os.getpid():>6}] END  ({processed_items} processed)")
        del self.calculator

    def get_infos(self):
        return self.psutil_proc.cpu_percent(), getattr(self.psutil_proc.memory_info(), var.MEMORY_ATTR) / 1073741824


class ProcessPool(object):
    def __init__(self, calculator, graph_kwargs, max_workers, chunksize, workers_wait_time, **options):
        self.calculator, self.graph_kwargs = calculator, graph_kwargs
        self.max_workers, self.chunksize, self.workers_wait_time = max_workers, chunksize, workers_wait_time
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
            time.sleep(self.workers_wait_time / self.max_workers)
            process = WorkerProcess(number=i,
                                    calculator=self.calculator, graph_kwargs=self.graph_kwargs,
                                    input_queue=self.input_queue, output_queue=self.output_queue)
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
                logging.warning(f"Process {process.name} is dead, trying to restart...")
                manager.print_status()
                unfinished = process.get_current_item()
                if unfinished is not None:
                    self.input_queue.put(unfinished)
                process.close()
                process = WorkerProcess(number=i,
                                        calculator=self.calculator, graph_kwargs=self.graph_kwargs,
                                        input_queue=self.input_queue, output_queue=self.output_queue)
                process.start()
                self.processes[i] = process

    def update(self, manager):
        self._check_processes(manager)
        self._update_cache(manager)
        return len(self.cache)

    def get_infos(self):
        infos = tuple(zip(*(proc.get_infos() for proc in self.processes)))
        cumulatives = ((sum(infos[0]) + self.psutil_proc.cpu_percent()) / 100,
                       sum(getattr(p.memory_info(), var.MEMORY_ATTR) for p in
                           (self.psutil_proc, *self.psutil_proc.children(recursive=True))) / 1073741824)
        return infos, cumulatives

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()
