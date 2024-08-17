import enlighten
import os, math, time


class Manager(enlighten.Manager):
    PROGRESSBAR_FORMAT = u'     {percentage_2:3.0f}% |{bar}|' + \
                         u' {count_2:{len_total}d}+{count_1}+{count_0}/{total:d} ' + \
                         u'[{elapsed}<{eta_2}, {interval_2:.2f}s]'
    INFOBARS_FORMAT = "{type} {cumulative:5.1f} | {values} |{post}"
    LOGBAR_FORMAT = "{type:<9} | {value}  {status}"
    INFOBARS = ["CPU", "RAM"]

    def __init__(self, total, **kwargs):
        super().__init__(**kwargs)
        # self.committed, self.cached, self.loaded = None, None, None
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
        infos, cumulatives = executor.get_infos()
        max_vals = (os.get_terminal_size().columns - 20) // 6
        values = (
            "  ".join(
                f"{int(s):>4}" for s in infos[0][self.curr_info * max_vals:(self.curr_info + 1) * max_vals]),
            "  ".join(f"{s:>4.1f}" for s in infos[1][self.curr_info * max_vals:(self.curr_info + 1) * max_vals]),
        )
        posts = (f" page {self.curr_info + 1}",
                 f"{self.curr_info * max_vals:3}-{min(len(infos[0]), (self.curr_info + 1) * max_vals) - 1:3}")
        for infobar, cumul, val, post in zip(self.infobars.values(), cumulatives, values, posts):
            infobar.update(cumulative=cumul, values=val, post=post)
        self.curr_info = (self.curr_info + 1) % math.ceil(len(infos[0]) / max_vals)
        self.loaded.refresh()

    def add_log(self, type="", value="", status=""):
        self.logbar.update(type=type, value=value, status=status)

    def change_log_status(self, status):
        self.logbar.update(status=status)

    def update_loaded(self, num=1):
        return self.loaded.update(num)

    def update_cached(self, num=1):
        if self.cached.count == 0:
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
