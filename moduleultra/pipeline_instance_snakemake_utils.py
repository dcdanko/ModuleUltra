import sys


def initialImports():
    preprocessed = ''
    # add imports
    preprocessed += 'import os.path\n'
    preprocessed += 'import datasuper as ds\n'
    preprocessed += 'from moduleultra.snakemake_utils import *\n'
    return preprocessed


def wildcardConstraints():
    regex = '[a-zA-Z0-9_-]'
    constraints = '''
    wildcard_constraints:
        sample_name="{}",
        group_name="{}",
    '''.format(regex, regex)
    return constraints


def makeSnakemakeAllRule(endpts, samples, groups):
    allRule = 'rule all:\n\tinput: config["final_inputs"]\n'
    return allRule
    ruleBldr = SnakemakeRuleBuilder('all')
    ruleBldr.addInput("config['final_inputs']")
    return str(ruleBldr)


def addFinalPatternsToConf(conf, endpts, samples, groups):
    allInps = []
    for schema in endpts:
        if schema.isOrigin():
            continue
        pattern = schema.getOutputFilePattern()
        if schema.level == 'SAMPLE':
            for sample in samples:
                allInps.append(pattern.format(sample_name=sample.name))
        elif schema.level == 'GROUP':
            for group in groups:
                allInps.append(pattern.format(group_name=group.name))
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
    originConf = {origin: {} for origin in origins}

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
