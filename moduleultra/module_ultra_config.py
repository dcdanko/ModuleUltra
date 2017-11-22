from yaml_backed_structs import *
import os.path
import os
from json import loads as jloads
from .errors import *
from .installation import *
from shutil import rmtree


class ModuleUltraConfig:
    '''
    This class represents the module ultra config
    directory and lots of associated operations.

    Typically this directory is in $HOME/.module_ultra_config
    '''
    configDirName = '.module_ultra_config'
    pipelineDirName = 'installed_pipelines'
    stagingDirName = 'staging'
    pipelineSetName = 'installed_pipelines.yml'
    configVarsRoot = 'config_variables.yml'

    def __init__(self, abspath):
        self.abspath = abspath

        varPath = os.path.join(self.abspath, ModuleUltraConfig.configVarsRoot)
        self.configVars = PersistentDict(varPath)

        pipePath = os.path.join(self.abspath,
                                ModuleUltraConfig.pipelineSetName)
        self.installedPipes = PersistentDict(pipePath)

    def listInstalledPipelines(self):
        return {k: v for k, v in self.installedPipes.items()}

    def getPipelineDefinition(self, pipelineName, version=None):
        '''
        Return the definition for the specified pipeline if installed.

        If the version is not specified return the highest version number'
        '''
        if version is None:
            version = getHighestVersion(self.installedPipes[pipelineName])

        pipeName = joinPipelineNameVersion(pipelineName, version)
        pipeDef = os.path.join(self.getInstalledPipelinesDir(), pipeName)
        pipeDef = os.path.join(pipeDef, 'pipeline_definition.json')
        pipeDef = open(pipeDef).read()
        pipeDef = jloads(pipeDef)
        return pipeDef

    def clusterSubmitScript(self):
        try:
            cScript = self.configVars['CLUSTER_SUBMIT_SCRIPT']
            return cScript
        except KeyError:
            return None

    def getInstalledPipelinesDir(self):
        return os.path.join(self.abspath, ModuleUltraConfig.pipelineDirName)

    def installPipeline(self, uri, dev=False):
        installer = PipelineInstaller(self, uri, dev=dev)
        installer.install()

    def uninstallPipeline(self, pipeName, version=None):
        if version is None:
            version = getHighestVersion(self.installedPipes[pipeName])

        vPipeName = joinPipelineNameVersion(pipeName, version)
        pipeDir = os.path.join(self.getInstalledPipelinesDir(), vPipeName)
        if os.path.islink(pipeDir):
            os.unlink(pipeDir)
        else:
            rmtree(pipeDir)
        del self.installedPipes[pipeName]

    def getSnakefile(self, pipeName, version, fileName):
        vPipeName = joinPipelineNameVersion(pipeName, version)
        pipeDir = os.path.join(self.getInstalledPipelinesDir(), vPipeName)
        pipeDef = os.path.join(pipeDir, 'pipeline_definition.json')
        pipeDef = jloads(open(pipeDef).read())
        try:
            snakeDir = pipeDef["SNAKEMAKE"]["DIR"]
            snakeDir = os.path.join(pipeDir, snakeDir)
        except KeyError:
            snakeDir = pipeDir
        snakeFile = os.path.join(snakeDir, fileName)
        return snakeFile

    def getSnakemakeConf(self, pipeName, version):
        vPipeName = joinPipelineNameVersion(pipeName, version)
        pipeDir = os.path.join(self.getInstalledPipelinesDir(), vPipeName)
        pipeDef = os.path.join(pipeDir, 'pipeline_definition.json')
        pipeDef = jloads(open(pipeDef).read())
        try:
            snakeConf = pipeDef["SNAKEMAKE"]["CONF"]
            snakeConf = os.path.join(pipeDir, snakeConf)
        except KeyError:
            snakeConf = os.path.join(pipeDir, 'snakemake_config.json')
        return snakeConf

    @classmethod
    def getConfigDir(ctype):
        '''
        Return the directory where the module ultra config is
        stored
        '''
        try:
            configRoot = os.environ['MODULE_ULTRA_CONFIG']
        except KeyError:
            configRoot = ModuleUltraConfig.configDirName
            configRoot = os.path.join(os.environ['HOME'], configRoot)
        return os.path.abspath(configRoot)

    @classmethod
    def load(ctype):
        '''
        Get the config object
        '''
        return ModuleUltraConfig(ctype.getConfigDir())

    @classmethod
    def initConfig(ctype, dest=None):
        try:
            os.mkdir(ctype.getConfigDir())

            pipeDir = os.path.join(ctype.getConfigDir(), ctype.pipelineDirName)
            os.mkdir(pipeDir)

            stagingDir = os.path.join(pipeDir, ctype.stagingDirName)
            os.mkdir(stagingDir)

        except FileExistsError:
            raise ModuleUltraConfigAlreadyExists()
