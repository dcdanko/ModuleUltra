from blessings import Terminal


DEFAULT_BAR_WIDTH = 70


class CompactProgressBars:

    def __init__(self, rulename, njobs, terminal, width=DEFAULT_BAR_WIDTH):
        self.terminal = terminal
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
            self.nremaining,
            self.nrunning,
            self.term.green,
            self.nfinished,
            self.term.red,
            self.nerrors,
        ])
        job_width = self.width - len(nums)
        if job_width < len(self.rulename):
            display_rulename = self.rulename[:job_width]
        else:
            spacer = (job_width - len(self.rulename)) * ' '
            display_rulename = self.rulename + spacer
        out = display_rulename + nums + self.term.normal
        assert len(out) == self.width
        return out


class CompactMultiProgressBars:

    def __init__(self):
        self.progress_bars = {}
        self.term = Terminal()

    def update(self):
        """Render the current state on screen."""
        ncols = self.term.width // (DEFAULT_BAR_WIDTH + 1)
        col_width = (self.term.width - ncols + 1) // ncols
        with self.term.location(0, 0):
            line = ''
            for i, cpbar in enumerate(self.progress_bars.values()):
                cpbar.width = col_width
                if (i % ncols) == (ncols - 1):
                    line += str(cpbar)
                    print(line)
                else:
                    line += str(cpbar) + ' '

    def handle_msg(self, msg):
        """Send a logger message to the approprite function."""
        level = msg['level']
        handlers = {
            'job_info': self.handle_job_info,
            'job_error': self.handle_job_error,
            'job_finished': self.handle_job_complete,
            'run_info': self.handle_run_info,
        }
        if level in handlers:
            handlers[level](msg)
            self.update()

    def handle_run_info(self, msg):
        """Grab initial job counts, otherwise do nothing."""
        msg = msg['msg']
        if self.progress_bars or 'Job counts:' not in msg:
            return
        line_list = msg.split('\n')[2:]  # first two lines are cruft
        line_list = [el.strip().split('\t') for el in line_list]
        job_list = [
            (tkns[0], int(tkns[1]))
            for tkns in line_list if len(tkns) == 2
        ]
        for rulename, count in job_list:
            self.progress_bars[rulename] = CompactProgressBars(
                rulename, count, self.term)

    def handle_job_info(self, msg):
        """Indicate a job has been started."""
        rulename = msg['name']
        self.progress_bars[rulename].started()

    def handle_job_error(self, msg):
        """Indicate a job is in error."""
        rulename = msg['name']
        self.progress_bars[rulename].errored()

    def handle_job_complete(self, msg):
        """Indicate a job is complete."""
        rulename = msg['name']
        self.progress_bars[rulename].finished()
