from blessings import Terminal
from multiprocessing import Lock

DEFAULT_BAR_WIDTH = 60

class ProgressBar:

    def __init__(self, name, total, terminal):
        self.name = name
        self.total = total
        self.term = terminal
        self.done = 0
        self.errored = 0
        self.started = 0
        self.width = DEFAULT_BAR_WIDTH

    def __str__(self):
        max_num_width = len(f'| ({self.total} / {self.total})')
        num = f'| ({self.done} / {self.total})'
        front = f'{self.name}: |'
        bar_width = self.width - (len(front) + max_num_width) - 1
        done_filled = int(bar_width * (self.done / self.total))
        error_filled = int(bar_width * (self.errored / self.total))
        start_filled = int(bar_width * (self.started / self.total))
        remain = bar_width - done_filled - error_filled - start_filled
        bar = self.term.green + done_filled * '#' + self.term.red + error_filled * 'X' + self.term.normal + start_filled * '-' + remain * ' '
        full_bar = front + bar + num
        return full_bar


class CompactProgressBar:

    def __init__(self, rulename, njobs, terminal, width=DEFAULT_BAR_WIDTH):
        self.term = terminal
        self.rulename = rulename
        self.nremaining = njobs
        self.nrunning = 0
        self.nfinished = 0
        self.nerrors = 0
        self.width = width

    def started(self):
        self.nrunning += 1
        self.nremaining -= 1

    def errored(self):
        self.nrunning -= 1
        self.nerrors += 1

    def finished(self):
        self.nrunning -= 1
        self.nfinished += 1

    def __str__(self):
        nums = ' ' + ' '.join([
            str(self.nremaining),
            str(self.nrunning),
            self.term.green + str(self.nfinished),
            self.term.red + str(self.nerrors),
        ])
        job_width = self.width - len(nums) - 1
        if job_width < len(self.rulename):
            display_rulename = self.rulename[:job_width]
        else:
            spacer = (job_width - len(self.rulename)) * ' '
            display_rulename = self.rulename + spacer

        assert len(display_rulename) == job_width, f'actual {len(display_rulename)} desired {job_width}'
        out = display_rulename + nums + self.term.normal
        nspecial_chars = 3
        assert len(out) == (self.width + nspecial_chars), f'actual {len(out)} desired {self.width}'
        return out


class CompactMultiProgressBars:

    def __init__(self, name=None):
        self.progress_bars = {}
        self.term = Terminal()
        self.name = name
        self.master_progress = None
        self.jobids = {}
        self.lock = Lock()

    def update(self):
        """Render the current state on screen."""
        ncols = 2  # self.term.width // (DEFAULT_BAR_WIDTH + 2)
        col_width = (self.term.width - 2 * (ncols - 1)) // ncols
        print(self.term.clear)
        with self.term.location(0, 0):
            if self.name:
                print(self.name)
                print()
            self.master_progress.width = self.term.width
            print(self.master_progress)
            print(f'Finished: {self.master_progress.done} Errored: {self.master_progress.errored} Outstanding: {self.master_progress.started}')
            print()
            line = ''
            for i, cpbar in enumerate(self.progress_bars.values()):
                cpbar.width = col_width
                if (i % ncols) == (ncols - 1):
                    line += str(cpbar)
                    print(line)
                    line = ''
                else:
                    line += str(cpbar) + '  '
            print(line)

    def handle_msg(self, msg):
        self.lock.acquire()
        self._handle_msg(msg)
        self.lock.release()

    def _handle_msg(self, msg):
        """Send a logger message to the appropriate function."""
        level = msg['level']
        handlers = {
            'job_info': self.handle_job_info,
            'job_error': self.handle_job_error,
            'job_finished': self.handle_job_complete,
            'run_info': self.handle_run_info,
            'progress': self.handle_progress,
        }
        if level in handlers:
            handlers[level](msg)
            self.update()

    def handle_progress(self, msg):
        return
        done = msg['done']
        self.master_progress.done = done

    def handle_run_info(self, msg):
        """Grab initial job counts, otherwise do nothing."""
        msg = msg['msg']
        if self.progress_bars or 'Job counts:' not in msg:
            return
        line_list = msg.split('\n')[2:]  # first two lines are cruft
        line_list = [el.strip().split('\t') for el in line_list]
        job_list = [
            (tkns[1], int(tkns[0]))
            for tkns in line_list if len(tkns) == 2
        ]
        total = 0
        for rulename, count in job_list:
            total += count
            self.progress_bars[rulename] = CompactProgressBar(
                rulename, count, self.term)
        self.master_progress = ProgressBar('Jobs', total, self.term)
        print(self.term.clear)

    def handle_job_info(self, msg):
        """Indicate a job has been started."""
        rulename = msg['name']
        jobid = msg['jobid']
        self.jobids[jobid] = rulename
        self.progress_bars[rulename].started()
        self.master_progress.started += 1

    def handle_job_error(self, msg):
        """Indicate a job is in error."""
        rulename = msg['name']
        self.progress_bars[rulename].errored()
        self.master_progress.errored += 1
        self.master_progress.started -= 1

    def handle_job_complete(self, msg):
        """Indicate a job is complete."""
        jobid = msg['jobid']
        rulename = self.jobids[jobid]
        self.progress_bars[rulename].finished()
        self.master_progress.done += 1
        self.master_progress.started -= 1
