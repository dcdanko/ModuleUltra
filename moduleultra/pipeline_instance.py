from snakemake import snakemake
from .utils import *
from datasuper.utils import parsers as dsparsers
from .result_schema import ResultSchema
import subprocess as sp
import json
import sys
import datasuper as ds
from .snakemake_rule_builder import SnakemakeRuleBuilder


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
            endpts=None, groups=None, samples=None,
            dryrun=False, unlock=False, jobs=1, local=False):

        dsRepo = ds.Repo.loadRepo()
        if not groups:
            groups = dsRepo.db.sampleGroupTable.getAll()
        else:
            groups = dsRepo.db.sampleGroupTable.getMany(groups)
        if not samples:
            samples = dsRepo.db.sampleTable.getAll()
        else:
            samples = dsRepo.db.sampleTable.getMany(samples)
        if not endpts:
            endpts = [schema for schema in self.resultSchema if schema.name in self.endpoints]
        else:
            endpts = [schema for schema in self.resultSchema if schema.name in endpts]

            
        preprocessedConf = self.preprocessConf(self.origins, samples, groups)
        snakefile = self.preprocessSnakemake( preprocessedConf, endpts, samples, groups)
        clusterScript = None
        if not local:
            clusterScript = self.muRepo.muConfig.clusterSubmitScript()
            
        snakemake( snakefile,
                   config={},
                   workdir=self.muRepo.getResultDir(),
                   cluster=clusterScript,
                   keepgoing=True,
                   printshellcmds=True,
                   dryrun=dryrun,
                   unlock=unlock,
                   force_incomplete=True,
                   nodes=jobs)


    def preprocessSnakemake(self, confStr, endpts, samples, groups):
        preprocessed = ''

        # add imports
        preprocessed += 'import os.path\n'
        preprocessed += 'import datasuper as ds\n'
        preprocessed += 'from moduleultra.snakemake_utils import *\n'
        
        # add conf
        preprocessed += '\nconfig={}\n\n'.format(confStr)
        
        # add all rule
        preprocessed += self.makeSnakemakeAllRule( endpts, samples, groups)

        # add individual results
        for resultSchema in self.resultSchema:
            if resultSchema.isOrigin():
                continue
            preprocessed += resultSchema.preprocessSnakemake()
            preprocessed += '\n'

        preprocessed = tabify( preprocessed)
        
        # write to a file
        sfile = self.muRepo.snakemakeFilepath(self.pipelineName)
        with open(sfile, 'w') as sf:
            sf.write(preprocessed)
            
        return sfile

    def makeSnakemakeAllRule(self, endpts, samples, groups):
        ruleBldr = SnakemakeRuleBuilder('all')
        for schema in endpts:
            pattern = schema.getOutputFilePattern()
            if schema.level == 'SAMPLE':
                for sample in samples:
                    inp = pattern.format(sample_name=sample.name)
                    ruleBldr.addInput(inp)
            elif schema.level == 'GROUP':
                for group in groups:
                    inp = pattern.format(group_name=group.name)
                    ruleBldr.addInput(inp)
            else:
                print(schema.level, file=sys.stderr)
                assert False
        return str(ruleBldr)

    def preprocessConf(self, origins, samples, groups):
        pconf = json.loads(open(self.snakemakeConf).read())
        pconf = runBackticks( pconf)
        for resultSchema in self.resultSchema:
            resultSchema.preprocessConf(pconf)

        pconf = self.addDataToSnakemakeConf(pconf, samples, groups)
        pconf = self.addOriginsToSnakemakeConf(pconf, origins, samples, groups)

        
        confStr = json.dumps(pconf, indent=4)
        '''
        # this is a bad hack that should probably be forgotten
        confLines = confStr.split('\n')
        newConfLines = []
        for confLine in confLines:
            tkns = confLine.split(' ')
            newTkns = []
            for i, tkn in enumerate(tkns):
                if '$' in tkn:
                    newTkn = ''            
                    for c in tkn:
                        if c not in ['"', '$']:
                            newTkn += c
                else:
                    newTkn = tkn

                newTkns.append( newTkn)
            newConfLines.append( ' '.join(newTkns))
        
        confStr = '\n'.join(newConfLines)
        print( confStr)            
        '''
        return confStr


    def addDataToSnakemakeConf(self, conf, samples, groups):
        sampleConf = {}
        for sample in samples:
            sampleConf[sample.name] = {'sample_type': sample.sampleType}
        conf['samples'] = sampleConf
        return conf


    def addOriginsToSnakemakeConf(self, conf, origins, samples, groups):
        originConf = { origin : {} for origin in origins}

        for sample in samples:
            for result in sample.results(resultTypes=origins):
                recs = {}
                for fileRecName, fileRec in result.files():
                    recs[fileRecName] = fileRec.filepath()
                originConf[result.resultType()][sample.name] = recs
        for group in groups:
            for result in group.allResults(resultTypes=origins):
                recs = {}
                for fileRecName, fileRec in result.files():
                    recs[fileRecName] = fileRec.filepath()
                originConf[result.resultType()][group.name] = recs
        conf['origins'] = originConf
        return conf

    def listFileTypes(self):
        return [el for el in self.fileTypes]

    def listEndpoints(self):
        return self.endpoints

    def listResultSchema(self):
        return [schema for schema in self.resultSchema]

    def listSampleTypes(self):
        return [el for el in self.sampleTypes]




def tabify( s):
    tabwidth = 4
    tabtoken = ' '*tabwidth
    out = ''
    for line in s.split('\n'):
        prefix = ''
        newLine = ''
        inPrefix = True
        for c in line:
            if inPrefix and (c == ' '):
                prefix += ' '
            elif inPrefix and (c == '\t'):
                prefix += tabtoken
            elif inPrefix:
                inPrefix = False
            if not inPrefix:
                newLine += c
        newPrefix = '\t'* ( (len(prefix) + tabwidth - 1) // tabwidth)
        out += newPrefix
        out += newLine
        out += '\n'
    return out
    

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
                   try:
                       cmdOut = sp.check_output(bticks, shell=True)
                       out += cmdOut.decode('utf-8').strip()                       
                   except sp.CalledProcessError:
                       print('subcommand "{}" failed'.format(bticks))
                       out += '""'
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
        
