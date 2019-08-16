
from random import choice
from os import chdir, getcwd
from multiprocessing import Pool, TimeoutError

from .config import DaemonConfig


def repo_status(daemon_config=None, timeout=3600):
    """Return a dict of repo_config -> [((pipeline_name, version), number_outstanding_jobs)]."""
    daemon_config = daemon_config if daemon_config else DaemonConfig.load_from_yaml()
    original_dir = getcwd()
    pool = Pool(daemon_config.total_jobs)
    async_results = [
        pool.apply_async(handle_one_repo, (repo_config, daemon_config, original_dir))
        for repo_config in daemon_config.list_repos()
    ]
    for result in async_results:
        try:
            yield result.get(timeout)
        except TimeoutError:
            pass


def handle_one_repo(repo_config, daemon_config, original_dir):
    try:
        chdir(repo_config.repo_path)
        return repo_config, list(_status_one_repo(daemon_config, repo_config))
    except Exception:
        pass
    finally:
        chdir(original_dir)


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
            if rulename == 'all':
                continue
            self.num_outstanding_jobs += count


def _status_one_repo(daemon_config, repo_config):
    for pipe_name, pipe_version in repo_config.get_pipeline_list():
        try:
            yield (pipe_name, pipe_version), _status_one_repo_one_pipeline(
                daemon_config, repo_config, pipe_name, pipe_version
            )
        except Exception:
            raise
            yield (pipe_name, pipe_version), -1000


def _status_one_repo_one_pipeline(daemon_config, repo_config, pipe_name, pipe_version):
    repo = repo_config.get_repo()
    try:
        pipe = repo.getPipelineInstance(pipe_name)
    except AssertionError:
        repo.addPipeline(pipe_name, version=pipe_version)
        pipe = repo.getPipelineInstance(pipe_name)
    assert pipe.pipelineVersion == pipe_version
    count = _SnakemakeInfoGrabber()
    pipe.run(
        endpts=repo_config.get_pipeline_endpts(pipe_name),
        excludeEndpts=repo_config.get_pipeline_excluded_endpts(pipe_name),
        local=daemon_config.run_local,
        custom_config_file=daemon_config.get_pipeline_run_config(pipe_name, pipe_version),
        dryrun=True,
        logger=lambda x: x,
        loghandler=count.handle_msg
    )
    return count.num_outstanding_jobs


def repo_run(daemon_config=None):
    """Run unfished pipelines in the repo.

    Only run one pipeline per repo at a time.
    """
    daemon_config = daemon_config if daemon_config else DaemonConfig.load_from_yaml()
    jobs_per_repo = daemon_config.total_jobs / len(daemon_config.repos)
    for repo_config, pipelines in repo_status(daemon_config=daemon_config):
        try:
            _run_one_repo(daemon_config, repo_config, pipelines, jobs_per_repo)
        except Exception:
            continue


def _run_one_repo(daemon_config, repo_config, pipelines, njobs):
    (pipe_name, pipe_version), _ = choice([
        p for p in pipelines if p[1] > repo_config.get_pipeline_tolerance(p[0][0])
    ])
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
