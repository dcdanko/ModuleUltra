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
from .snakemake_log_handler import CompactMultiProgressBars
from os import getcwd


class PipelineInstance:
    '''Represents an instance of a pipeline in a repo.

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

        self.origins = pipelineDef['ORIGINS']
        self.flatOrigins = []
        for origin_group in self.origins:
            if type(origin_group) == str:
                origin_group = [origin_group]
            for origin in origin_group:
                self.flatOrigins.append(origin)

        self.resultSchema = []
        for schema in pipelineDef['RESULT_TYPES']:
            isOrigin = schema['NAME'] in self.flatOrigins
            self.resultSchema.append(ResultSchema(muRepo,
                                                  self.pipelineName,
                                                  self.pipelineVersion,
                                                  schema,
                                                  origin=isOrigin))

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
            dryrun=False, reason=True, unlock=False, jobs=1, local=False,
            custom_config_file=None, compact_logger=False, benchmark=False,
            logger=None):
        '''Run this pipeline.

        To do this:
            Get correct lists of samples and groups to be processed.
            Get correct list of endpoints to be processed.
            Build a conf for the master snakefile.
            Build a master snakefile.
            Get the cluster submission script.
            Get the jobname template.
            Run snakemake.

        Args:
            endpts (:obj:`[str]`, optional): A list of endpoints that should
                be run. If None run all endpoints.
            excludeEndpts (:obj:`[str]`, optional): A list of endpoints that
                should not be run. Takes precedence over anything in
                endpoints. If None, do not filter any endpoints.
            groups (:obj:`[str]`, optional): A list of groups to process. All
                samples in each group will also be processed. May be a list of
                strings or datasuper group objects.
            samples (:obj:`[str]`, optional): A list of samples to process.
                May be a list of strings or datasuper group objects.
            dryrun (:obj:`bool`, optional): Do not actually run the pipeline.
                Just print out a list of jobs that would be run.
            unlock (:obj:`bool`, optional): Unlock the snakemake directory.
                Do nothing else.
            jobs (:obj:`int`, optional): The number of jobs that should be
                run at once. Defaults to one.
            local (:obj:`bool`, optional): Run all jobs on the local machine.
                Defaults to False.
        '''
        if not logger:
            logger = lambda s: print(s, file=sys.stderr)
        if benchmark:
            for schema in self.resultSchema:
                schema.benchmark = True
        samples, groups = preprocessSamplesAndGroups(self.origins,
                                                     samples, groups)
        endpts = self.preprocessEndpoints(endpts, excludeEndpts)
        preprocessedConf = self.preprocessConf(
            self.origins,
            samples,
            groups,
            endpts,
            custom_config_file=custom_config_file
        )
        endpt_names = ', '.join([endpt.name for endpt in endpts])
        logger(f'Running Endpoints: {endpt_names}')
        snakefile = self.preprocessSnakemake(preprocessedConf,
                                             endpts,
                                             samples,
                                             groups)
        clusterScript = self.getClusterSubmitScript(local)
        snkmkJobnameTemplate = self.getSnakemakeJobnameTemplate()

        loghandler = None
        if compact_logger:
            name = f'{getcwd()} :: {self.pipelineName} :: {self.pipelineVersion}'
            loghandler = CompactMultiProgressBars(name=name).handle_msg

        cores = 1
        if local:
            cores = jobs

        snakemake(
            snakefile,
            config={},
            workdir=self.muRepo.getResultDir(),
            cluster=clusterScript,
            keepgoing=True,
            printshellcmds=True,
            dryrun=dryrun,
            printreason=reason,
            unlock=unlock,
            force_incomplete=True,
            latency_wait=100,
            jobname=snkmkJobnameTemplate,
            nodes=jobs,
            log_handler=loghandler,
            cores=cores,
        )

    def getSnakemakeJobnameTemplate(self):
        '''Return a jobname template based on this pipeline instance.'''
        snkmkJobnameTemplate = ('MUJOB_',
                                self.pipelineName,
                                '_{rulename}',
                                '_{jobid}')
        snkmkJobnameTemplate = ''.join(snkmkJobnameTemplate)
        return snkmkJobnameTemplate

    def getClusterSubmitScript(self, local):
        '''Return the cluster submit script to use for jobs.'''
        clusterScript = None
        if not local:
            clusterScript = self.muRepo.muConfig.clusterSubmitScript()
            clusterScript += ' {}'.format(int(time()))
        return clusterScript

    def preprocessEndpoints(self, endpts, excludeEndpts):
        '''Return the correct list of endpoints to run.

        If `endpts` is not None only return endpoints that are
        in `endpts` but never return endpoints in `excludeEndpts`.
        '''
        if not endpts:
            endpts = {schema for schema in self.resultSchema if schema.name in self.endpoints}
        else:
            endpts = {schema for schema in self.resultSchema if schema.name in endpts}
        if excludeEndpts:
            endpts = {schema for schema in endpts if schema.name not in excludeEndpts}

        num_endpts = -1
        while len(endpts) != num_endpts:
            num_endpts = len(endpts)
            endpt_names = {schema.name for schema in endpts}
            new_endpts = set()
            for schema in endpts:
                all_in = True
                for depends in schema.dependencies:
                    if depends not in endpt_names:
                        all_in = False
                if all_in:
                    new_endpts.add(schema)
            endpts = new_endpts

        return list(endpts)

    def preprocessSnakemake(self, confStr, endpts, samples, groups):
        '''Return the abspath to a master snakefile that can be run.'''
        preprocessed = initialImports()
        preprocessed += wildcardConstraints()
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

    def preprocessConf(self, origins, samples, groups, endpts,
                       custom_config_file=None):
        '''Make a config object and return a JSON str of that object.'''
        pconf = openConfF(self.snakemakeConf)
        if custom_config_file:
            customConf = openConfF(custom_config_file)
            pconf = mergeConfs(customConf, pconf)
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

    def listOrigins(self):
        '''Return a list of origin endpoints in this pipeline.'''
        return self.flatOrigins

    def listFileTypes(self):
        '''Return a list of file types in this pipeline.'''
        return [el for el in self.fileTypes]

    def listEndpoints(self):
        '''Return a list of endpoints in this pipeline.'''
        return self.endpoints

    def listResultSchema(self):
        '''Return a list of result schema in this pipeline.'''
        return [schema for schema in self.resultSchema]

    def listSampleTypes(self):
        '''Return a list of sample types in this pipeline.'''
        return [el for el in self.sampleTypes]
