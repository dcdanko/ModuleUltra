from snakemake import snakemake
from .utils import *
from datasuper.utils import parsers as dsparsers
from .result_schema import ResultSchema
import subprocess as sp
import json
import sys

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
                                                   schema,
            ))
            
        self.origins = pipelineDef['ORIGINS']            
        for schema in self.resultSchema:
            if schema.name in self.origins:
                schema.origin = True
        
        allEnds = [schema.name for schema in self.resultSchema if schema.name not in self.origins]
        self.endpoints = getOrDefault( pipelineDef, 'END_POINTS', allEnds)
                    
        self.snakemakeConf = self.muConfig.getSnakemakeConf(self.pipelineName,
                                                             self.pipelineVersion)

    def run(self,
            endpts=None, groups=None, samples=None, results=None,
            dryrun=False, unlock=False, jobs=1, local=False):
        
        preprocessedConf = self.preprocessConf(endpts, groups, samples, results)
        snakefile = self.preprocessSnakemake(preprocessedConf)
        clusterScript = None
        if not local:
            clusterScript = self.muRepo.muConfig.clusterSubmitScript()
            
        snakemake( snakefile,
                   config={},
                   cluster=clusterScript,
                   keepgoing=True,
                   printshellcmds=True,
                   dryrun=dryrun,
                   unlock=unlock,
                   force_incomplete=True,
                   nodes=jobs)

    def preprocessConf(self, endpts, groups, samples, results):
        pconf = json.loads(open(self.snakemakeConf).read())
        pconf = runBackticks( pconf)
        for resultSchema in self.resultSchema:
            resultSchema.preprocessConf(pconf)
        confWithData = self.addEndpointsToSnakemakeConf(pconf, endpts)
        confWithData = self.addDataToSnakemakeConf(pconf, groups, samples, results)        
        return confWithData

        
    def preprocessSnakemake(self, conf):
        preprocessed = ''

        # add imports
        preprocessed += 'import os.path\n'
        preprocessed += 'import datasuper as ds\n'
        
        # add conf
        preprocessed += '\nconfig={}\n\n'.format(json.dumps(conf, indent=4))
        
        # add all rule

        # add individual results
        for resultSchema in self.resultSchema:
            if resultSchema.isOrigin():
                continue
            preprocessed += resultSchema.preprocessSnakemake()
            preprocessed += '\n'

        # write to a file
        sfile = self.muRepo.snakemakeFilepath(self.pipelineName)
        with open(sfile, 'w') as sf:
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

    
    def addEndpointsToSnakemakeConf(self, conf, endpts):
        resultDir = self.muRepo.getResultDir()
        return conf

    def addDataToSnakemakeConf(self, conf, groups, samples, results):
        resultDir = self.muRepo.getResultDir()
        return conf

    

def runBackticks(obj):
    if type(obj) == dict:
        out = {}
        for k,v in obj.items():
            out[k] = runBackticks(v)
        return out
    elif type(obj) == list:
        out = []
        for el in obj:
            out.append( runBackticks(el))
        return out
    elif type(obj) == str:
        if '`' not in obj:
            return obj
        out = ''
        bticks = ''
        inTicks = False
        for c in obj:
           if c == '`':
               if inTicks:
                   cmdOut = sp.check_output(bticks, shell=True)
                   out += cmdOut.decode('utf-8').strip()
               inTicks = not inTicks
           elif inTicks:
               bticks += c
           else:
               out += c
        return out
    elif type(obj) == int:
        return str(obj)
    else:
        print(type(obj), file=sys.stderr)
        print(obj, file=sys.stderr)        
        assert False # panic
        
