from snakemake import snakemake
from .utils import *
from datasuper.utils import parsers as dsparsers
from .result_schema import ResultSchema

class PipelineInstance:
    '''
    This is the class that handles actually running the pipeline.

    This is also used for other more basic calls (like seeing what 
    end points exist) which is why running is not automatic.

    Basically this class does a lot of setup work then calls
    snakemake
    '''
    
    def __init__(self, muRepo, pipeName, pipeVersion, pipelineDef):
        self.muRepo = muRepo
        self.muConfig = self.muRepo.muConfig
        self.pipelineName = pipeName
        self.pipelineVersion = pipeVersion
        
        self.fileTypes = dsparsers.parseFileTypes(pipelineDef['FILE_TYPES'])
        self.sampleTypes = pipelineDef['SAMPLE_TYPES']
        self.resultSchema = []
        for schema in pipelineDef['RESULT_TYPES']:
            self.resultSchema.append( ResultSchema(muRepo,
                                                   self.pipelineName,
                                                   self.pipelineVersion,
                                                   schema) )
        
        self.origins = pipelineDef['ORIGINS']
        allEnds = [schema.name for schema in self.resultSchema if schema.name not in self.origins]
        self.endpoints = getOrDefault( pipelineDef, 'END_POINTS', allEnds)

        self.snakemakeConf = self.muConfig.getSnakemakeConf(self.pipelineName,
                                                             self.pipelineVersion)

    def run(self,
            endpts=None, groups=None, samples=None, results=None,
            dryrun=False, unlock=False, njobs=1, local=False):
        snakefile = self.preprocessSnakemake()
        clusterScript = None
        if not local:
            clusterScript = self.muRepo.muConfig.clusterSubmitScript()
        confWithData = self.addEndpointsAndDataToSnakemakeConf( endpts, groups, samples, results)

        snakemake( snakefile,
                   config=confWithData,
                   cluster=clusterScript,
                   keepgoing=True,
                   printshellcmds=True,
                   dryrun=dryrun,
                   unlock=unlock,
                   force_incomplete=True,
                   nodes=njobs)

    def preprocessSnakemake(self):
        # add conf

        # add all rule

        # add individual results
        preprocessed = ''
        for resultSchema in self.resultSchema:
            preprocessed += resultSchema.preprocessSnakemake()
            preprocessed += '\n'
        sfile = self.muRepo.snakemakeFilepath(self.pipelineName)
        with open(tfile, 'w') as sf:
            sf.write(preprocessed)
        return sfile

    def listFileTypes(self):
        return [el for el in self.fileTypes]

    def listEndpoints(self):
        return self.endpoints

    def listResultSchema(self):
        return [schema for schema in self.resultSchema]

    def listSampleTypes(self):
        return [el for el in self.sampleTypes]

    
    def addEndpointsAndDataToSnakemakeConf(self, endpts, groups, samples, results):
        resultDir = self.muRepo.getResultDir()
        pass
