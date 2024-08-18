from modules import var
import enlighten, psutil
import os, math, time


def _colorize(val, term, type, proc_num=None, avg=False):
    if avg:
        val = val / proc_num if proc_num else 0.0
    if type == "CPU":
        s = f"{val:3.0f}" if avg else f"{val:4.0f}"
        if val <= 5:
            return term.red(s)
        elif val >= 90:
            return term.green(s)
        return term.yellow(s)
    elif type == "RAM":
        thresh = var.TOTAL_RAM / proc_num
        s = f"{val:3.1f}" if avg else f"{val:4.1f}"
        if val <= 0.9 * thresh:
            return term.green(s)
        elif 0.9 * thresh <= val <= thresh:
            return term.yellow(s)
        return term.red(s)


class Manager(enlighten.Manager):
    INFOBARS = ["CPU", "RAM"]

    def _load_formats(self):
        self.PROGRESSBAR_FORMAT = u'      {percentage_2:3.0f}% (' + \
                                  self.term.cyan('{percentage_0:3.0f}%') + ') |{bar}|' + u' {count_2:{len_total}d}+' + \
                                  self.term.blue('{count_1}') + '+' + self.term.cyan('{count_0}') + \
                                  '/{total:d} ' + u'[{elapsed}<{eta_2}, {interval_2:.2f}s]'
        self.INFOBARS_FORMAT = self.term.black_on_white("{type}") + " {cumulative}|{values}|{post}"
        self.LOGBAR_FORMAT = self.term.black_on_white(
            "{type:<18}") + "|{value:<32}  {status:<4}" + "{filler}" + "({children:>3} children)"

    def __init__(self, total, **kwargs):
        super().__init__(**kwargs)
        # self.committed, self.cached, self.loaded = None, None, None
        self._load_formats()
        self.total = total
        self.progbar = self.counter(total=total, bar_format=self.PROGRESSBAR_FORMAT,
                                    unit='graphs', color='cyan', leave=False, position=3)
        self.cached = self.progbar.add_subcounter('blue', all_fields=True)
        self.committed = self.progbar.add_subcounter('white', all_fields=True)

        self.infobars = dict((info, self.status_bar(status_format=self.INFOBARS_FORMAT,
                                                    type=info, cumulative=0, values="", post="",
                                                    poisition=i + 1, leave=False)) for i, info in
                             enumerate(self.INFOBARS))
        self.curr_info = 0
        self.logbar = self.status_bar(status_format=self.LOGBAR_FORMAT, type="", value="", status="", children=0,
                                      filler="",
                                      position=4, leave=False)
        self._update_logbar_filler()

    def _update_logbar_filler(self):
        self.logbar.update(filler=" " * max(0, os.get_terminal_size().columns - 71))

    def update(self, executor):
        infos, cumul_vals = executor.get_infos()
        proc_num = len(infos[0])
        max_vals = (os.get_terminal_size().columns - 27) // 6
        max_pag = math.ceil(proc_num / max_vals)
        cumulatives = (f"{cumul_vals[0] :5.0f}%  ({_colorize(cumul_vals[0], self.term, "CPU", proc_num, True)}) ",
                       f"{cumul_vals[1]:5.1f}Gb ({_colorize(cumul_vals[1], self.term, "RAM", proc_num, True)}) ")
        values = (
            "  ".join(_colorize(v, self.term, "CPU") for v in
                      infos[0][self.curr_info * max_vals:(self.curr_info + 1) * max_vals]),
            "  ".join(_colorize(v, self.term, "RAM", proc_num) for v in
                      infos[1][self.curr_info * max_vals:(self.curr_info + 1) * max_vals]),
        )
        posts = (f"{self.curr_info + 1:^3}/{max_pag:^3}",
                 f"{self.curr_info * max_vals:^3}-{min(len(infos[0]), (self.curr_info + 1) * max_vals) - 1:^3}")
        for infobar, cumul, val, post in zip(self.infobars.values(), cumulatives, values, posts):
            infobar.update(cumulative=cumul, values=val, post=post)
        self.curr_info = (self.curr_info + 1) % max_pag
        self.logbar.update(children=len(psutil.Process().children(recursive=True)))
        self._update_logbar_filler()
        self.progbar.refresh()

    def add_log(self, type="", value="", status=""):
        self.logbar.update(type=type, value=value, status=status)

    def change_log_status(self, status):
        self.logbar.update(status=status)

    def update_loaded(self, num=1):
        return self.progbar.update(num)

    def update_cached(self, num=1):
        if self.committed.count == 0:
            self.progbar.start = time.time()
        return self.cached.update_from(self.progbar, num)

    def update_committed(self, num=1):
        return self.committed.update_from(self.cached, num)

    def get_loaded_count(self):
        return self.progbar.count - self.progbar.subcount

    def print_status(self):
        for infobar in self.infobars.values():
            print(infobar)

    def finished(self):
        return self.cached.count + self.committed.count == self.total

    def stop(self):
        self.progbar.close()
        for infobar in self.infobars.values():
            infobar.close()
        self.logbar.close()
        super().stop()
