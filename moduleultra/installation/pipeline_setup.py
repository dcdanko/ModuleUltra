import shutil
from subprocess import call
import os.path
from os import symlink
from yaml import load as yload
from moduleultra.utils import *
from moduleultra.errors import PipelineAlreadyInstalledError
import packagemega as pm
from gimme_input import BoolUserInput


class PipelineInstaller:
    '''
    Installs a pipeline
    '''

    stagingDir = 'staging'

    def __init__(self, muConfig, uri, dev=False):
        self.uri = uri
        self.muConfig = muConfig
        self.dev = dev

    def install(self):
        staged = self.stagePipeline()
        pipeDef = self.provisionallyLoadPipeline(staged)
        pipeDir = self.loadPipelineFilesIntoConfig(staged, pipeDef)
        self.installPyPiDependencies(pipeDef)
        self.installCondaDependencies(pipeDef)
        self.runPipelineRecipes(pipeDef, pipeDir)
        self.addPipelineToManifest(pipeDef)

    def stagePipeline(self):
        if os.path.exists(self.uri):
            return self.stageFromLocal()
        elif 'git' in self.uri:
            return self.stageFromGithub()

    def stageFromLocal(self):
        dest = self.muConfig.getInstalledPipelinesDir()
        dest = os.path.join(dest, PipelineInstaller.stagingDir)
        uri = os.path.abspath(self.uri)
        if uri[-1] == '/':
            uri = uri[:-1]
        hname = uri.split('/')[-1].split('.')[0]
        dest = os.path.join(dest, hname)
        if self.dev:
            symlink(self.uri, dest)
        else:
            shutil.copytree(self.uri, dest)
        return os.path.join(dest)

    def stageFromGithub(self):
        if self.dev:
            assert False and 'Dev mode can only be applied to local pipelines'
        dest = self.muConfig.getInstalledPipelinesDir()
        dest = os.path.join(dest, PipelineInstaller.stagingDir)
        hname = self.uri.split('/')[-1].split('.')[0]
        dest = os.path.join(dest, hname)
        cmd = 'git clone {} {}'.format(self.uri, dest)
        call(cmd, shell=True)
        return os.path.join(dest)

    def provisionallyLoadPipeline(self, staged):
        for ext in ['yml', 'yaml', 'json']:
            pipeDef = os.path.join(staged, 'pipeline_definition.' + ext)
            if os.path.isfile(pipeDef):
                break
        with open(pipeDef) as pD:
            pipeDef = pD.read()
        pipeDef = yload(pipeDef)
        return pipeDef

    def loadPipelineFilesIntoConfig(self, staged, pipeDef):
        version = pipeDef['VERSION']
        pipeDir = joinPipelineNameVersion(pipeDef['NAME'], version)
        dest = self.muConfig.getInstalledPipelinesDir()
        dest = os.path.join(dest, pipeDir)
        shutil.move(staged, dest)
        return dest

    def installPyPiDependencies(self, pipeDef):
        pass

    def installCondaDependencies(self, pipeDef):
        pass

    def addPipelineToManifest(self, pipeDef):
        pipeName = pipeDef['NAME']
        pipeVersion = pipeDef['VERSION']
        if pipeName in self.muConfig.installedPipes:
            if pipeVersion in self.muConfig.installedPipes[pipeName]:
                raise PipelineAlreadyInstalledError()
            self.muConfig.installedPipes[pipeName] += [pipeVersion]
        else:
            self.muConfig.installedPipes[pipeName] = [pipeVersion]

    def runPipelineRecipes(self, pipeDef, pipeDir):
        try:
            recipeDir = pipeDef['PACKAGE_MEGA']['RECIPE_DIR']
        except KeyError:
            return
        recipeDir = os.path.join(pipeDir, recipeDir)
        pmRepo = pm.Repo.loadRepo()
        recipes = pmRepo.addFromLocal(recipeDir, dev=self.dev)
        installedRecipes = pmRepo.allRecipes()
        nInstall = len([recipe for recipe in recipes if recipe in installedRecipes])
        inp = BoolUserInput('Skip {} recipes that are already installed?'.format(nInstall), True)
        doskip = (nInstall > 0) and inp.resolve()
        for recipe in recipes:
            if (not doskip) or (recipe not in installedRecipes):
                pmRepo.makeRecipe(recipe)
