from yaml_backed_structs import *
from .utils import *
from .errors import *
import os.path
import datasuper as ds
from .module_ultra_config import ModuleUltraConfig
from .pipeline_instance import PipelineInstance


class ModuleUltraRepo:
    '''
    Represents a directory where moduleultra pipelines are run
    '''

    repoDirName = '.module_ultra'
    resultDirName = 'core_results'
    pipeRoot = 'pipelines.yml'

    def __init__(self, abspath):
        self.abspath = abspath
        self.muConfig = ModuleUltraConfig.load()

        pipePath = os.path.join(self.abspath, ModuleUltraRepo.pipeRoot)
        self.pipelines = PersistentDict(pipePath)

    def datasuperRepo(self):
        return ds.Repo.loadRepo()

    def addPipeline(self, pipelineName, version=None, modify=False):
        '''
        Add a pipeline that has already been installed
        to this repo.

        Add relevant types to the datasuper repo.
        Add the pipeline to the list of pipelines in the repo.
        '''
        if not modify:
            if pipelineName in self.pipelines:
                raise PipelineAlreadyInRepoError()

        pipelineDef = self.muConfig.getPipelineDefinition(pipelineName,
                                                          version=version)
        if version is None:
            version = pipelineDef["VERSION"]

        self.addPipelineTypes(pipelineName, version, pipelineDef)    

        self.pipelines[pipelineName] = version

    def addPipelineTypes(self, pipelineName, version, pipelineDef):
        instance = PipelineInstance(self, pipelineName, version, pipelineDef)
        with ds.Repo.loadRepo() as dsRepo:
            for fileTypeName in instance.listFileTypes():
                dsRepo.addFileType(fileTypeName)

            for schema in instance.listResultSchema():
                dsRepo.addResultSchema(schema.name, schema.files)

            for sampleTypeName in instance.listSampleTypes():
                dsRepo.addSampleType(sampleTypeName)

    def getPipelineInstance(self, pipelineName, version=None):
        assert pipelineName in self.pipelines
        if version is not None:
            assert version == self.pipelines[pipelineName]
        else:
            version = self.pipelines[pipelineName]

        pipelineDef = self.muConfig.getPipelineDefinition(pipelineName,
                                                          version=version)
        return PipelineInstance(self, pipelineName, version, pipelineDef)

    def listPipelines(self):
        '''
        List the names of pipelines that have been added to this repo
        '''
        return [p for p in self.pipelines.keys()]

    def snakemakeFilepath(self, pipelineName):
        '''
        Return the path to use for the snakemake file
        '''
        snakeFile = 'snakemake_{}.smk'.format(pipelineName)
        return os.path.join(self.abspath, snakeFile)

    def getResultDir(self):
        '''
        Get the directory where the actual result files are stored.
        '''
        return os.path.join(self.abspath, ModuleUltraRepo.resultDirName)

    def makeVirtualSampleDir(self, dname, sample):
        '''
        Create a directory named <dname>
        with all the results for a given sample
        '''
        pass

    def makeVirtualGroupDir(self, dname, group, flat=False):
        '''
        Create a directory named <dname> with all the
        results for a given groups

        unless flat is true make subdirectories for
        each sample plus a 'group_result' dir
        '''
        pass

    @staticmethod
    def repoDir(startDir='.'):
        '''
        returns the abspath for the .module_ultra directory
        '''
        startPath = os.path.abspath(startDir)
        if ModuleUltraRepo.repoDirName in os.listdir(startPath):
            repoPath = os.path.join(startPath, ModuleUltraRepo.repoDirName)
            return repoPath
        up = os.path.dirname(startPath)
        if up == startPath:
            raise NoModuleUltraRepoFoundError()
        return ModuleUltraRepo.repoDir(startDir=up)

    @staticmethod
    def loadRepo(startDir='.'):
        repoPath = ModuleUltraRepo.repoDir(startDir=startDir)
        return ModuleUltraRepo(repoPath)

    @staticmethod
    def initRepo(root='.'):
        '''
        Create a datasuper repo in the same root
        Create a .module_ultra directory
        Create a .module_ultra/core_results directory
        '''
        try:
            ds.Repo.initRepo(targetDir=root)
        except ds.RepoAlreadyExistsError:
            pass  # this is fine

        try:
            p = os.path.abspath(root)
            p = os.path.join(p, ModuleUltraRepo.repoDirName)
            os.makedirs(p)
            p = os.path.join(p, ModuleUltraRepo.resultDirName)
            os.makedirs(p)
        except FileExistsError:
            raise ModuleUltraRepoAlreadyExists()

        return ModuleUltraRepo.loadRepo(startDir=root)
