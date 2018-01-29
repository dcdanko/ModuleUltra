from snakemake import snakemake
from .utils import *
from datasuper.utils import parsers as dsparsers
from .result_schema import ResultSchema
import json
import sys
import datasuper as ds
from .snakemake_rule_builder import SnakemakeRuleBuilder
from time import time
from .pipeline_instance_utils import *
from .pipeline_instance_snakemake_utils import *


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
        '''
        if 'RESULT_DEFINITIONS' in pipelineDef:
            for fname in schema['RESULT_DEFINITIONS']:
                schema = loadResultDefinition(fname)
                self.resultSchema.append(ResultSchema(muRepo,
                                                      self.pipelineName,
                                                      self.pipelineVersion,
                                                      schema))
        '''
        for schema in pipelineDef['RESULT_TYPES']:
            self.resultSchema.append(ResultSchema(muRepo,
                                                  self.pipelineName,
                                                  self.pipelineVersion,
                                                  schema))

        self.origins = pipelineDef['ORIGINS']
        for schema in self.resultSchema:
            if schema.name in self.origins:
                schema.origin = True

        allEnds = [schema.name
                   for schema in self.resultSchema
                   if schema.name not in self.origins]
        self.endpoints = getOrDefault(pipelineDef, 'END_POINTS', allEnds)

        self.snakemakeConf = self.muConfig.getSnakemakeConf(self.pipelineName,
                                                            self.pipelineVersion)

    def run(self,
            endpts=None, excludeEndpts=None, groups=None, samples=None,
            dryrun=False, unlock=False, jobs=1, local=False):

        dsRepo = ds.Repo.loadRepo()
        if not groups:
            groups = dsRepo.db.sampleGroupTable.getAll()
            if not samples:
                samples = dsRepo.db.sampleTable.getAll()
            else:
                samples = dsRepo.db.sampleTable.getMany(samples)
        else:
            groups = dsRepo.db.sampleGroupTable.getMany(groups)
            if not samples:
                samples = []
                for group in groups:
                    samples += group.samples()
            else:
                samples = dsRepo.db.sampleTable.getMany(samples)

        if not endpts:
            endpts = [schema
                      for schema in self.resultSchema
                      if schema.name in self.endpoints]
        else:
            endpts = [schema
                      for schema in self.resultSchema
                      if schema.name in endpts]
        if excludeEndpts:
            endpts = [schema
                      for schema in self.resultSchema
                      if schema.name not in excludeEndpts]

        preprocessedConf = self.preprocessConf(self.origins,
                                               samples,
                                               groups,
                                               endpts)
        snakefile = self.preprocessSnakemake(preprocessedConf,
                                             endpts,
                                             samples,
                                             groups)
        clusterScript = None
        if not local:
            clusterScript = self.muRepo.muConfig.clusterSubmitScript()
            clusterScript += ' {}'.format(int(time()))

        snkmkJobnameTemplate = ('MUJOB_',
                                self.pipelineName,
                                '_{rulename}',
                                '_{jobid}')

        snkmkJobnameTemplate = ''.join(snkmkJobnameTemplate)

        snakemake(snakefile,
                  config={},
                  workdir=self.muRepo.getResultDir(),
                  cluster=clusterScript,
                  keepgoing=True,
                  printshellcmds=True,
                  dryrun=dryrun,
                  unlock=unlock,
                  force_incomplete=True,
                  latency_wait=100,
                  jobname=snkmkJobnameTemplate,
                  nodes=jobs)

    def preprocessSnakemake(self, confStr, endpts, samples, groups):
        preprocessed = initialImports()
        preprocessed += '\nconfig={}\n\n'.format(confStr)  # add conf
        preprocessed += makeSnakemakeAllRule(endpts, samples, groups)

        # add individual results
        for resultSchema in self.resultSchema:
            if (resultSchema in endpts) and (not resultSchema.isOrigin()):
                preprocessed += resultSchema.preprocessSnakemake()
                preprocessed += '\n'
        preprocessed = tabify(preprocessed)

        # write to a file
        sfile = self.muRepo.snakemakeFilepath(self.pipelineName)
        with open(sfile, 'w') as sf:
            sf.write(preprocessed)
        return sfile

    def preprocessConf(self, origins, samples, groups, endpts):
        confF = self.snakemakeConf
        ext = confF.split('.')[-1]
        if ext == 'json':
            pconf = openJSONConf(confF)
        elif ext == 'py':
            pconf = openPythonConf(confF)
        pconf = runBackticks(pconf)
        for resultSchema in self.resultSchema:
            if resultSchema in endpts:
                resultSchema.preprocessConf(pconf)

        pconf = addFinalPatternsToConf(pconf, endpts, samples, groups)
        pconf = addDataToSnakemakeConf(pconf, samples, groups)
        pconf = addOriginsToSnakemakeConf(pconf, origins, samples, groups)
        pipeDir = self.muConfig.getPipelineDir(self.pipelineName,
                                               self.pipelineVersion)
        pconf['pipeline_dir'] = pipeDir

        confStr = json.dumps(pconf, indent=4)

        return confStr

    def listFileTypes(self):
        return [el for el in self.fileTypes]

    def listEndpoints(self):
        return self.endpoints

    def listResultSchema(self):
        return [schema for schema in self.resultSchema]

    def listSampleTypes(self):
        return [el for el in self.sampleTypes]

