import json
import yaml
import subprocess as sp
import sys
import os.path
from inspect import getmembers
import datasuper as ds


def mergeConfs(priority, base):
    out = {}
    for k, v in base.items():
        if k not in priority:
            out[k] = v
        else:
            if type(v) == dict:
                out[k] = mergeConfs(priority[k], base[k])
            else:
                out[k] = priority[k]
    return out


def openConfF(confF):
    ext = confF.split('.')[-1]
    if ext == 'json':
        pconf = openJSONConf(confF)
    elif ext == 'py':
        pconf = openPythonConf(confF)
    return pconf


def openJSONConf(confF):
    '''Read a JSON file and return the deserialized object.'''
    return json.loads(open(confF).read())


def preprocessSamplesAndGroups(origins, samples, groups):
    '''Return the appropriate list of samples and groups.

    If `groups` is None return a list of all available groups.
    If `samples` is None return a list of all the samples
    implied by `groups`. Otherwise return the samples in `samples`.

    Return everything as DataSuper records, not strings.
    '''

    dsRepo = ds.Repo.loadRepo()
    if groups is None:
        groups = dsRepo.db.sampleGroupTable.getAll()
        if samples is None:
            samples = dsRepo.db.sampleTable.getAll()
        else:
            samples = dsRepo.db.sampleTable.getMany(samples)
    else:
        groups = dsRepo.db.sampleGroupTable.getMany(groups)
        if samples is None:
            samples = []
            for group in groups:
                samples += group.allSamples()
        else:
            samples = dsRepo.db.sampleTable.getMany(samples)

    filteredSamples = []
    for sample in samples:
        rtypes = {result.resultType() for result in sample.results()}
        keep = True
        for origin_group in origins:
            if type(origin_group) == str:
                origin_group = [origin_group]
            has_group = False
            for origin in origin_group:
                if origin in rtypes:
                    has_group = True
            if not has_group:
                keep = False
        if keep:
            filteredSamples.append(sample)

    sampleSet = {sample.name for sample in filteredSamples}
    filteredGroups = []
    for group in groups:
        keep = True
        for sample in group.allSamples():
            if sample not in sampleSet:
                keep = False
        if keep:
            filteredGroups.append(group)

    return filteredSamples, filteredGroups


def openPythonConf(confF):
    '''Read and resolve a python config file. Return the result.'''
    importName = os.path.basename(confF)[:-3]
    sys.path.append(os.path.dirname(confF))
    __import__(importName)
    members = getmembers(sys.modules[importName])
    for name, member in members:
        if name == 'config':
            config = member
    return config


def loadResultDefinition(fname):
    '''Deserialize `fname` appropriate to extension. Return the result.'''
    ext = fname.split('.')[-1]
    fstr = open(fname).read()
    if ext == 'json':
        return json.loads(fstr)
    elif ext == 'yaml':
        return yaml.loads(fstr)


def tabify(s):
    '''Convert all groups of 4 spaces in `s` to tabs. Return the result.'''
    tabwidth = 4
    tabtoken = ' ' * tabwidth
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
        newPrefix = '\t' * ((len(prefix) + tabwidth - 1) // tabwidth)
        out += newPrefix
        out += newLine
        out += '\n'
    return out


def runBackticks(obj):
    '''Resolve all commands between bacticks in a JSONable object.'''
    if type(obj) == dict:
        out = {}
        for k, v in obj.items():
            out[k] = runBackticks(v)
        return out
    elif type(obj) == list:
        out = []
        for el in obj:
            out.append(runBackticks(el))
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
                        print('subcommand "{}" failed'.format(bticks),
                              file=sys.stderr)
                        out += '""'
                inTicks = not inTicks
            elif inTicks:
                bticks += c
            else:
                out += c
        return out
    elif type(obj) in [int, float]:
        return str(obj)
    else:
        print(type(obj), file=sys.stderr)
        print(obj, file=sys.stderr)
        assert False  # panic
