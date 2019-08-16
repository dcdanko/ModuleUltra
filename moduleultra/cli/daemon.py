"""CLI commands for various daemon utils."""

import click

from time import gmtime, strftime

from moduleultra.daemon import repo_status, repo_run
from moduleultra.utils import joinPipelineNameVersion


@click.group()
def daemon():
    pass


@daemon.command('status')
def cli_daemon_status():
    """Print the status of all repos in the config."""
    for repo_config, pipelines in repo_status():
        header = f'{repo_config.repo_name} {repo_config.repo_path}'
        for (pipe_name, version), num_jobs in pipelines:
            timestamp = strftime("%Y-%m-%d %H:%M:%S", gmtime())
            pipe = joinPipelineNameVersion(pipe_name, version)
            print(f'[{timestamp}] {header} {pipe} {num_jobs}')


@daemon.command('run')
def cli_daemon_run():
    """Run unfinished pipelines in the config."""
    repo_run()
