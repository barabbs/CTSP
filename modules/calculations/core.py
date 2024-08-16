import os, time
from modules import var


class Calculation(object):
    CALC_TYPE = None
    CALC_NAME = None

    def __init__(self, **kwargs):
        # print(f"{os.getpid() - os.getppid():<4} - Initializing calculation {self.CALC_NAME}")
        pass

    def _initialize(self):
        pass

    def initialize(self):
        self._initialize()

    def _close(self):
        pass

    def close(self):
        self._close()

    def _calc(self, graph):
        return dict()

    def calc(self, graph):
        start = time.process_time_ns()
        result = self._calc(graph)
        result.update({var.TIMINGS_TABLE: {self.CALC_TYPE: time.process_time_ns() - start}})
        return result
