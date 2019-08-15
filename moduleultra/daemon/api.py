
from random import choice

from .config import DaemonConfig


def repo_status(daemon_config=DaemonConfig.load_from_yaml()):
    """Return a dict of repo_config -> [((pipeline_name, version), number_outstanding_jobs)]."""
    status = {}
    for repo_config in daemon_config.list_repos():
        try:
            status_of_repo = _status_one_repo(daemon_config, repo_config)
            status[repo_config] = list(status_of_repo.items())
        except Exception:
            continue
    return status_of_repo


class _SnakemakeInfoGrabber:

    def __init__(self):
        self.num_outstanding_jobs = -1

    def handle_msg(self, msg):
        if msg['level'] != 'run_info':
            return
        msg = msg['msg']
        line_list = msg.split('\n')[2:]  # first two lines are cruft
        line_list = [el.strip().split('\t') for el in line_list]
        job_list = [
            (tkns[1], int(tkns[0]))
            for tkns in line_list if len(tkns) == 2
        ]
        self.num_outstanding_jobs = 0
        for rulename, count in job_list:
            self.num_outstanding_jobs += count


def _status_one_repo(daemon_config, repo_config):
    repo = repo_config.get_repo()
    status = {}
    for pipe_name, pipe_version in repo_config.get_pipeline_list():
        try:
            pipe = repo.getPipelineInstance(pipe_name)
        except AssertionError:
            repo.addPipeline(pipe_name, version=pipe_version)
            pipe = repo.getPipelineInstance(pipe_name)
        assert pipe.pipelineVersion == pipe_version
        pipe.run(
            endpts=repo_config.get_pipeline_endpts(pipe_name),
            excludeEndpts=repo_config.get_pipeline_excluded_endpts(pipe_name),
            local=daemon_config.run_local,
            custom_config_file=daemon_config.get_pipeline_run_config(pipe_name, pipe_version),
            dryrun=True,
            loghandler=_SnakemakeInfoGrabber
        )
        status[(pipe_name, pipe_version)] = _SnakemakeInfoGrabber.num_outstanding_jobs
    return status


def repo_run(daemon_config=DaemonConfig.load_from_yaml()):
    """Run unfished pipelines in the repo.

    Only run one pipeline per repo at a time.
    """
    jobs_per_repo = daemon_config.total_jobs / len(daemon_config.repos)
    for repo_config, pipelines in repo_status(daemon_config=daemon_config):
        try:
            _run_one_repo(daemon_config, repo_config, pipelines, jobs_per_repo)
        except Exception:
            continue


def _run_one_repo(daemon_config, repo_config, pipelines, njobs):
    (pipe_name, pipe_version), _ = choice([p for p in pipelines if p[1] > 0])
    repo = repo_config.get_repo()
    try:
        pipe = repo.getPipelineInstance(pipe_name)
    except AssertionError:
        repo.addPipeline(pipe_name, version=pipe_version)
        pipe = repo.getPipelineInstance(pipe_name)
    assert pipe.pipelineVersion == pipe_version
    pipe.run(
        endpts=repo_config.get_pipeline_endpts(pipe_name),
        excludeEndpts=repo_config.get_pipeline_excluded_endpts(pipe_name),
        local=daemon_config.run_local,
        jobs=njobs,
        custom_config_file=daemon_config.get_pipeline_run_config(pipe_name, pipe_version),
    )
