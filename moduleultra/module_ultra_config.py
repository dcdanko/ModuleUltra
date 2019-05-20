from yaml_backed_structs import PersistentDict
import os.path
import os
from yaml import load as yload
from .errors import *
from .installation import *
from shutil import rmtree
from .utils import findFileInDirRecursively


class ModuleUltraConfig:
    '''This class represents a module ultra config.

    Currently the module ultra config can only be installed for
    a user, once. No global or specific installations are currently
    possible. As such this essentially a static class.


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
        '''Return the definition for the specified pipeline, if installed.

        If the version is not specified return the highest version number'
        '''
        if version is None:
            version = getHighestVersion(self.installedPipes[pipelineName])

        pipeName = joinPipelineNameVersion(pipelineName, version)
        pipeDefRoot = os.path.join(self.getInstalledPipelinesDir(), pipeName)
        for ext in ['yml', 'yaml', 'json']:
            pipeDef = os.path.join(pipeDefRoot, 'pipeline_definition.' + ext)
            if os.path.isfile(pipeDef):
                break
        pipeDef = open(pipeDef).read()
        pipeDef = yload(pipeDef)
        return pipeDef

    def setClusterSubmitScript(self, script):
        '''Ser the abspath for the cluster_submit_script.'''
        self.configVars['CLUSTER_SUBMIT_SCRIPT'] = os.path.abspath(script)

    def clusterSubmitScript(self):
        '''Return the abspath to the cluster submit script.'''
        try:
            cScript = self.configVars['CLUSTER_SUBMIT_SCRIPT']
            return cScript
        except KeyError:
            return None

    def getInstalledPipelinesDir(self):
        '''Return the abspath to the directory with installed pipelines.'''
        return os.path.join(self.abspath, ModuleUltraConfig.pipelineDirName)

    def installPipeline(self, uri, dev=False):
        '''Install a new pipeline.'''
        installer = PipelineInstaller(self, uri, dev=dev)
        installer.install()

    def uninstallPipeline(self, pipeName, version=None):
        '''Uninstall a pipeline.

        To do this:
            Find the directory where the pipeline is installed.
            Delete it

        Args:
            pipeName (str): The name of the pipeline to be removed.
            version (:obj:`str`, optional): Particular version to remove.
        '''

        if version is None:
            version = getHighestVersion(self.installedPipes[pipeName])

        vPipeName = joinPipelineNameVersion(pipeName, version)
        pipeDir = os.path.join(self.getInstalledPipelinesDir(), vPipeName)
        if os.path.islink(pipeDir):
            os.unlink(pipeDir)
        else:
            rmtree(pipeDir)
        del self.installedPipes[pipeName]

    def getPipelineDir(self, pipeName, version):
        '''Return the abspath of the installation directory for a pipeline.

        Args:
            pipeName (str): The name of the pipeline to be removed.
            version (:obj:`str`, optional): Particular version to remove.
        '''

        vPipeName = joinPipelineNameVersion(pipeName, version)
        pipeDir = os.path.join(self.getInstalledPipelinesDir(), vPipeName)
        return pipeDir

    def getSnakefile(self, pipeName, version, fileName):
        '''Return the abspath of a snakefile in a pipeline.

        ModuleUltra uses snakemake to run tools, one file per module.
        The names of these files are specified in each module (often
        by default). This function returns the path to the file as installed
        on the current system.

        Args:
            pipeName (str): The name of the pipeline.
            version (str): Pipeline version.
            fileName (str): The name of the snakefile.
        '''
        pipeDir = self.getPipelineDir(pipeName, version)
        pipeDef = self.getPipelineDefinition(pipeName, version=version)
        try:
            snakeDir = pipeDef["SNAKEMAKE"]["DIR"]
            snakeDir = os.path.join(pipeDir, snakeDir)
        except KeyError:
            snakeDir = pipeDir
        snakeFile = findFileInDirRecursively(snakeDir, fileName)
        return snakeFile

    def getSnakemakeConf(self, pipeName, version):
        '''Return the abspath of a config file for a pipeline.

        Most pipelines specify default config files. The paths
        to these config files are specified in the pipeline
        definition. This fucntion returns the abspath of the config
        file.

        Args:
            pipeName (str): The name of the pipeline.
            version (str): Pipeline version.
        '''
        pipeDir = self.getPipelineDir(pipeName, version)
        pipeDef = self.getPipelineDefinition(pipeName, version=version)
        try:
            snakeConf = pipeDef["SNAKEMAKE"]["CONF"]
            snakeConf = os.path.join(pipeDir, snakeConf)
        except KeyError:
            snakeConf = os.path.join(pipeDir, 'snakemake_config.json')
        return snakeConf

    @classmethod
    def getConfigDir(ctype):
        '''Return the abspath to module_ultra_config directory.'''
        try:
            configRoot = os.environ['MODULE_ULTRA_CONFIG']
        except KeyError:
            configRoot = ModuleUltraConfig.configDirName
            configRoot = os.path.join(os.environ['HOME'], configRoot)
        return os.path.abspath(configRoot)

    @classmethod
    def load(ctype):
        '''Return the ModuleUltraConfig.'''
        return ModuleUltraConfig(ctype.getConfigDir())

    @classmethod
    def initConfig(ctype, dest=None):
        '''Create the ModuleUltraConfig.'''
        try:
            os.mkdir(ctype.getConfigDir())

            pipeDir = os.path.join(ctype.getConfigDir(), ctype.pipelineDirName)
            os.mkdir(pipeDir)

            stagingDir = os.path.join(pipeDir, ctype.stagingDirName)
            os.mkdir(stagingDir)

        except FileExistsError:
            raise ModuleUltraConfigAlreadyExists()
