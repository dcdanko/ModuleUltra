from .utils import getOrDefault
import datasuper as ds
from .snakemake_rule_builder import SnakemakeRuleBuilder
class ResultSchema:

    def __init__(self,  muRepo, pipeName, pipeVersion, schema, origin=False):
        self.muRepo = muRepo
        self.muConfig = self.muRepo.muConfig
        self.pipelineName = pipeName
        self.pipelineVersion = pipeVersion
        self.origin=origin
        
        self.name = schema['NAME'] # this is the name of the result type in datasuper as well
        self.dependencies = getOrDefault( schema, 'DEPENDENCIES', [])
        self.module = getOrDefault( schema, 'MODULE', self.name)
        self.level = getOrDefault( schema, 'LEVEL', 'RESULT')

        self.snakeFilename = '{}.snkmk'.format(self.module)
        self.snakeFilepath = self.muConfig.getSnakefile(self.pipelineName,
                                                        self.pipelineVersion,
                                                        self.snakeFilename)
        self.files = {}
        files = schema['FILES']
        if type(files) == []:
            self.files = { str(i) : f for i, f in enumerate(files)}
        else:
            self.files = files    

            
    def makeRegisterRule(self):
        '''
        every result in ModuleUltra gets checked into data
        super for tracking. This is essentially boilerplate code
        which is entirely determined by the module.

        This is a little awkward but this function actually
        generates the string to make the snakemake rule that 
        registers the result. This is added to the pipeline
        definition at runtime.

        N.B. The result_name variable that snakemake has access to
        is not the same as the datasuper result_name. In practice it 
        is probably the same as the datasuper sample_name. This is 
        hacky but this whole approach is hacky.
        '''
        if self.level == 'RESULT':
            ruleBldr = SnakemakeRuleBuilder('register_{}'.format(self.module))

            dsRepo = ds.Repo.loadRepo()
            for fname, ftype in self.files.items():
                ext = dsRepo.getFileTypeExt(ftype)
                fpattern = self._makeFilePattern( fname, ext)
                ruleBldr.addInput(fname, fpattern)
            
            ruleBldr.setOutput( self._makeFilePattern('flag', 'registered'))            

            ruleBldr.addParam('dsResultName', self.module)
            ruleBldr.addParam('dsResultType', self.name)
            for fname, ftype in self.files.items():
                ruleBldr.addParam( fname, ftype)
            
            runStr =  '''
            with ds.Repo.loadRepo() as dsrepo:
                fileRecs = {}
                for fname, fpath in input.items():
                    name = os.path.basename(fpath)
                    fileType = params[fname]
                    fileRec = ds.FileRecord( dsrepo, name=name, filepath=fpath, file_type=fileType)
                    fileRec.save()
                    fileRecs[fname] = fileRec

                result = ds.ResultRecord( dsrepo,
                                          name=params.dsResultName,
                                          result_type=params.dsResultType,
                                          file_records=fileRecs)
                result.save()
                shell('touch '+output)

            '''
            ruleBldr.setRun(runStr)
            
            return str(ruleBldr)

    def preprocessSnakemake(self):
        '''
        Every result schema preprocesses its own snakefile 
        and returns it as a string

        Adds a register rule
        '''
        snakefileStr = open(self.snakeFilepath).read()
        snakefileStr += self.makeRegisterRule()
        return snakefileStr

    def isOrigin(self):
        return self.origin

    def _makeFilePattern(self, fname, ext):
        if self.level  == 'RESULT':
            fpattern = '{{result_name}}.{}.{}.{}'.format(self.module, fname, ext)
            return fpattern
        
    def preprocessConf(self, conf):
        dsRepo = ds.Repo.loadRepo()
        for fname, ftype in self.files.items():
            ext = dsRepo.getFileTypeExt(ftype)
            fpattern = self._makeFilePattern( fname, ext)
            try:
                conf[self.module][fname] = fpattern
            except KeyError:
                conf[self.module] = {fname: fpattern}
        return conf
