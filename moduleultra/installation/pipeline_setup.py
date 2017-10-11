import shutil
from subprocess import call
import os .path
from json import loads as jloads
from moduleultra.utils import *

class PipelineInstaller:
    '''
    Installs a pipeline
    '''

    stagingDir = 'staging'

    def __init__(self, muConfig, uri):
        self.uri = uri
        self.muConfig = muConfig


    def install(self):
        staged = self.stagePipeline()
        pipeDef = self.provisionallyLoadPipeline(staged)
        self.loadPipelineFilesIntoConfig(staged, pipeDef)
        self.installPyPiDependencies(pipeDef)
        self.installCondaDependencies(pipeDef)
        self.addPipelineToManifest(pipeDef)

    
        
    def stagePipeline(self):
        if os.path.exists(self.uri):
            return self.stageFromLocal()
        elif 'git' in self.uri:
            return self.stageFromGithub()
            
            
    def stageFromLocal(self):
        dest = self.muConfig.getInstalledPipelinesDir()
        dest = os.path.join( dest, PipelineInstaller.stagingDir)
        copytree(self.uri, dest)
        return os.path.join(dest)

    def stageFromGithub(self):
        dest = self.muConfig.getInstalledPipelinesDir()
        dest = os.path.join( dest, PipelineInstaller.stagingDir)
        hname = self.uri.split('/')[-1].split('.')[0]
        dest = os.path.join( dest, hname)
        cmd = 'git clone {} {}'.format(self.uri, dest)
        call(cmd, shell=True)
        return os.path.join(dest)

    def provisionallyLoadPipeline(self, staged):
        pipeDef = os.path.join(staged, 'pipeline_definition.json')
        with open(pipeDef) as pD:
            pipeDef = pD.read()
        pipeDef = jloads(pipeDef)
        return pipeDef
        
    def loadPipelineFilesIntoConfig(self, staged, pipeDef):
        pipeDir = joinPipelineNameVersion(pipeDef['NAME'], pipeDef['VERSION'])
        dest = self.muConfig.getInstalledPipelinesDir()
        dest = os.path.join( dest, pipeDir)
        shutil.move(staged, dest)
        
    
    def installPyPiDependencies(self, pipeDef):
        pass

    def installCondaDependencies(self, pipeDef):
        pass

    def addPipelineToManifest(self, pipeDef):
        pipeName = pipeDef['NAME']
        pipeVersion = pipeDef['VERSION']
        if pipeName in self.muConfig.installedPipes:
            if pipeVersion in self.muConfig.installedPipes[pipeName]:
                raise PipelineAlreadyInstalledException()
            self.muConfig.installedPipes[pipeName] += [pipeVersion]
        else:
            self.muConfig.installedPipes[pipeName] = [pipeVersion]
