import click
import sys
from moduleultra import *
from gimme_input import *

@click.group()
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
@click.option('--endpts/--all-endpts', default=False)
@click.option('--exclude-endpts/--no-exclude-endpts', default=False)
@click.option('--choose/--all', default=False)
@click.option('--local/--cluster', default=False)
@click.option('--dryrun/--wetrun', default=False)
@click.option('--unlock/--no-unlock', default=False)
@click.option('-j', '--jobs', default=1)
def runPipe(pipeline, version, endpts, exclude_endpts, choose, local, dryrun, unlock, jobs):
    repo = ModuleUltraRepo.loadRepo()
    pipe = repo.getPipelineInstance(pipeline, version=version)
    dsRepo = repo.datasuperRepo()

    # select sets
    if endpts:
        endpts = UserMultiChoice('What end points should be evaluated?',
                                 pipe.listEndpoints()).resolve()
    if exclude_endpts:
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
        if groups is None:
            samplesToChooseFrom = []
            for group in groups:
                samplesToChooseFrom += group.samples()
        else:
            samplesToChooseFrom = dsRepo.db.sampleTable.getAll()
        samples = UserMultiChoice('What samples should data be taken from?',
                                  samplesToChooseFrom,
                                  display=lambda x: x.name).resolve()

    # run the pipeline
    pipe.run(endpts=endpts, excludeEndpts=excludedEndpts, groups=groups, samples=samples,
             dryrun=dryrun, unlock=unlock, local=local, jobs=jobs)


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
@click.argument('name', nargs=1)
def detailPipeline(name):
    pass


if __name__ == '__main__':
    main()
