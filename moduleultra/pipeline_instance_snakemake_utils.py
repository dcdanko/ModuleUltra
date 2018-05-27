import sys
from .snakemake_rule_builder import SnakemakeRuleBuilder


def initialImports():
    preprocessed = ''
    # add imports
    preprocessed += 'import os.path\n'
    preprocessed += 'import datasuper as ds\n'
    preprocessed += 'from moduleultra.snakemake_utils import *\n'
    preprocessed += '\nglobal_master_datasuper_repo = ds.Repo.loadRepo()\n'
    return preprocessed


def wildcardConstraints():
    regex = '[a-zA-Z0-9_-]+'
    constraints = 'wildcard_constraints:\n'
    constraints += '    sample_name="{}",\n'.format(regex)
    constraints += '    group_name="{}",\n'.format(regex)
    return constraints


def makeDirBuilder():
    return '''
    
    import os

    for sname in config['samples'].keys():
        os.makedirs(sname, exist_ok=True)

    for gname in config['groups'].keys():
        os.makedirs(sname, exist_ok=True)
    '''


def makeSnakemakeAllRule(endpts, samples, groups):
    allRule = 'rule all:\n\tinput: inputsToAllRule(config)\n'
    return allRule


def addFinalPatternsToConf(conf, endpts, samples, groups):
    allInps = {'sample_patterns': [],
               'group_patterns': []}
    for schema in endpts:
        if schema.isOrigin():
            continue
        pattern = schema.getOutputFilePattern()
        if schema.level == 'SAMPLE':
            allInps['sample_patterns'].append(pattern)
            #for sample in samples:
            #    allInps['sample_names'].append(sample.name)
        elif schema.level == 'GROUP':
            allInps['group_patterns'].append(pattern)
            #for group in groups:
            #    allInps['group_names'].append(group.name)
        else:
            print(schema.level, file=sys.stderr)
            assert False
    conf['final_inputs'] = allInps
    return conf


def addDataToSnakemakeConf(conf, samples, groups):
    sampleConf = {}
    for sample in samples:
        sampleConf[sample.name] = {'sample_type': sample.sampleType}
    conf['samples'] = sampleConf
    groupConf = {}
    for group in groups:
        groupConf[group.name] = [sample.name
                                 for sample in group.allSamples()]
    conf['groups'] = groupConf

    return conf


def addOriginsToSnakemakeConf(conf, origins, samples, groups):
    flat_origins = []
    for origin_group in origins:
        if type(origin_group) == str:
            origin_group = [origin_group]
        for origin in origin_group:
            flat_origins.append(origin)

    originConf = {origin: {} for origin in flat_origins}

    for sample in samples:
        for result in sample.results(resultTypes=flat_origins):
            recs = {}
            for fileRecName, fileRec in result.files():
                recs[fileRecName] = fileRec.filepath()
            originConf[result.resultType()][sample.name] = recs
    for group in groups:
        for result in group.allResults(resultTypes=flat_origins):
            recs = {}
            for fileRecName, fileRec in result.files():
                recs[fileRecName] = fileRec.filepath()
            originConf[result.resultType()][group.name] = recs
    conf['origins'] = originConf
    return conf
