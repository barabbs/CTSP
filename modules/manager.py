import enlighten
import os, math, time


class Manager(enlighten.Manager):
    INFOBARS = ["CPU", "RAM"]

    def _load_formats(self):
        self.PROGRESSBAR_FORMAT = u'      {percentage_2:3.0f}% ({percentage_0:3.0f}%)  |{bar}|' + \
                                  u' {count_2:{len_total}d}+{count_1}+{count_0}/{total:d} ' + \
                                  u'[{elapsed}<{eta_2}, {interval_2:.2f}s]'
        self.INFOBARS_FORMAT = self.term.black_on_white("{type}") + " {cumulative} | {values} | {post}"
        self.LOGBAR_FORMAT = self.term.black_on_white("{type:<18}") + " | {value}  {status}"

    def __init__(self, total, **kwargs):
        super().__init__(**kwargs)
        # self.committed, self.cached, self.loaded = None, None, None
        self._load_formats()
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
        infos, cumul_vals = executor.get_infos()
        proc_num = len(infos[0])
        max_vals = (os.get_terminal_size().columns - 31) // 6
        max_pag = math.ceil(proc_num / max_vals)
        cumulatives = (f"{cumul_vals[0] :5.0f}%  ({cumul_vals[0] / proc_num if proc_num else 0.0:3.0f}) ",
                       f"{cumul_vals[1]:5.1f}Gb ({cumul_vals[1] / proc_num if proc_num else 0.0:3.1f}) ")
        values = (
            "  ".join(
                f"{int(s):>4}" for s in infos[0][self.curr_info * max_vals:(self.curr_info + 1) * max_vals]),
            "  ".join(f"{s:>4.1f}" for s in infos[1][self.curr_info * max_vals:(self.curr_info + 1) * max_vals]),
        )
        posts = (f"{self.curr_info + 1:^3}/{max_pag:^3}",
                 f"{self.curr_info * max_vals:^3}-{min(len(infos[0]), (self.curr_info + 1) * max_vals) - 1:^3}")
        for infobar, cumul, val, post in zip(self.infobars.values(), cumulatives, values, posts):
            infobar.update(cumulative=cumul, values=val, post=post)
        self.curr_info = (self.curr_info + 1) % max_pag
        self.loaded.refresh()

    def add_log(self, type="", value="", status=""):
        self.logbar.update(type=type, value=value, status=status)

    def change_log_status(self, status):
        self.logbar.update(status=status)

    def update_loaded(self, num=1):
        return self.loaded.update(num)

    def update_cached(self, num=1):
        if self.committed.count == 0:
            self.loaded.start = time.time()
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
