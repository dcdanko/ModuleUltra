"""CLI command definitions."""

import os
import sys
import click
from moduleultra import *
from gimme_input import *
from yaml import dump as ydump


version = {}
version_path = os.path.join(os.path.dirname(__file__), '../version.py')
with open(version_path) as version_file:
    exec(version_file.read(), version)


@click.group()
@click.version_option(version['__version__'])
def main():
    pass


@main.command()
def init():
    try:
        ModuleUltraConfig.initConfig()
    except ModuleUltraConfigAlreadyExists:
        pass  # this is fine

    try:
        ModuleUltraRepo.initRepo()
    except ModuleUltraRepoAlreadyExists:
        print('Repo already exists.', file=sys.stderr)


@main.group()
def config():
    pass


@config.command(name='cluster_submit')
@click.argument('script')
def setSubmitScript(script):
    muConfig = ModuleUltraConfig.load()
    muConfig.setClusterSubmitScript(script)

###############################################################################


@main.group()
def add():
    pass


@add.command(name='pipeline')
@click.option('-v', '--version', default=None, type=str)
@click.option('--modify/--no-modify', default=False)
@click.argument('name', nargs=1)
def addPipeline(version, modify, name):
    repo = ModuleUltraRepo.loadRepo()
    try:
        repo.addPipeline(name, version=version, modify=modify)
    except errors.PipelineAlreadyInRepoError:
        print('{} is already in this repo.'.format(name), file=sys.stderr)

###############################################################################


@main.command(name='install')
@click.option('--dev/--normal', default=False)
@click.argument('uri', nargs=1)
def installPipeline(uri, dev=False):
    muConfig = ModuleUltraConfig.load()
    try:
        muConfig.installPipeline(uri, dev=dev)
    except PipelineAlreadyInstalledError:
        print('Pipeline already installed.', file=sys.stderr)


@main.command(name='uninstall')
@click.option('-v', '--version', default=None, type=str)
@click.argument('name', nargs=1)
def uninstallPipeline(name, version=None):
    muConfig = ModuleUltraConfig.load()
    muConfig.uninstallPipeline(name, version=version)


@main.command(name='reinstall')
@click.option('-v', '--version', default=None, type=str)
@click.option('--dev/--normal', default=False)
@click.argument('name', nargs=1)
@click.argument('uri', nargs=1)
def reinstallPipeline(name, uri, version=None, dev=False):
    muConfig = ModuleUltraConfig.load()
    try:
        muConfig.uninstallPipeline(name, version=version)
    except KeyError:
        pass # pipeline not installed
    muConfig.installPipeline(uri, dev=dev)

###############################################################################


@main.command(name='run')
@click.option('-p', '--pipeline', default=None, type=str)
@click.option('-v', '--version', default=None, type=str)
@click.option('-c', '--local-config', default=None, type=str)
@click.option('--choose-endpts/--all-endpts', default=False)
@click.option('--choose-exclude-endpts/--no-exclude-endpts', default=False)
@click.option('--exclude-endpts', default='', type=str, help='list of comma-separated names')
@click.option('--choose/--all', default=False)
@click.option('--local/--cluster', default=True)
@click.option('--dryrun/--wetrun', default=False)
@click.option('--unlock/--no-unlock', default=False)
@click.option('--compact/--logger', default=False)
@click.option('--benchmark/--no-benchmark', default=False)
@click.option('-j', '--jobs', default=1)
def runPipe(pipeline, version, local_config,
            choose_endpts, choose_exclude_endpts, exclude_endpts, choose,
            local, dryrun, unlock, compact, benchmark, jobs):
    repo = ModuleUltraRepo.loadRepo()
    if pipeline is None:
        pipeline = UserChoice('pipeline', repo.listPipelines()).resolve()

    pipe = repo.getPipelineInstance(pipeline, version=version)
    print('Running {} :: {}'.format(pipe.pipelineName, pipe.pipelineVersion))

    dsRepo = repo.datasuperRepo()

    # select sets
    endpts = False
    if choose_endpts:
        endpts = UserMultiChoice('What end points should be evaluated?',
                                 pipe.listEndpoints()).resolve()

    excludedEndpts = exclude_endpts.split(',')
    if choose_exclude_endpts:
        excludedEndpts = UserMultiChoice('What end points should NOT be evaluated?',
                                 pipe.listEndpoints()).resolve()
    groups = None
    inp = BoolUserInput('Process data from specific sample groups?', False)
    if choose and inp.resolve():
        groups = UserMultiChoice('What sample groups should be processed?',
                                 dsRepo.db.sampleGroupTable.getAll(),
                                 display=lambda x: x.name).resolve()
    samples = None
    inp = BoolUserInput('Process data from a specific samples?', False)
    if choose and inp.resolve():
        if groups is not None:
            samplesToChooseFrom = []
            for group in groups:
                samplesToChooseFrom += group.samples()
        else:
            samplesToChooseFrom = dsRepo.db.sampleTable.getAll()
        samples = UserMultiChoice('What samples should data be taken from?',
                                  samplesToChooseFrom,
                                  display=lambda x: x.name).resolve()

    # run the pipeline
    pipe.run(endpts=endpts, excludeEndpts=excludedEndpts,
             groups=groups, samples=samples, dryrun=dryrun,
             unlock=unlock, local=local, jobs=jobs,
             custom_config_file=local_config, compact_logger=compact,
             benchmark=benchmark)


###############################################################################

@main.group(name='view')
def view():
    pass


@view.command(name='pipelines')
@click.option('--installed/--local', default=False)
def viewPipelines(installed):
    if installed:
        msg = '''
        # Showing all pipelines currently installed
        #
        # to add a pipeline to a repo navigate to the repo and run
        # moduleultra add <pipeline name>
        '''
        muConfig = ModuleUltraConfig.load()
        for pName, versions in muConfig.listInstalledPipelines().items():
            vs = ' '.join(versions)
            print('{} :: {}'.format(pName, vs))
    else:
        msg = '''
        # Showing pipelines currently in this repo
        # to see all installed pipelines use '--installed' flag
        '''
        print(msg)
        repo = ModuleUltraRepo.loadRepo()
        for pName in repo.listPipelines():
            print(pName)

###############################################################################


@view.group(name='detail')
def detail():
    pass


@detail.command(name='pipeline')
@click.option('-v', '--version', default=None, type=str)
@click.argument('name', nargs=1)
def detailPipeline(version, name):
    repo = ModuleUltraRepo.loadRepo()
    pipe = repo.getPipelineInstance(name, version=version)

    out = {
        'origins': pipe.listOrigins(),
        'endpoints': pipe.listEndpoints(),
    }
    click.echo(ydump(out))


if __name__ == '__main__':
    main()
