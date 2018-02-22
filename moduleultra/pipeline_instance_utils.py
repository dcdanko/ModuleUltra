import json
import yaml
import subprocess as sp
import sys
import os.path
from inspect import getmembers
import datasuper as ds


def openJSONConf(confF):
    '''Read a JSON file and return the deserialized object.'''
    return json.loads(open(confF).read())


def preprocessSamplesAndGroups(samples, groups):
    '''Return the appropriate list of samples and groups.

    If `groups` is None return a list of all available groups.
    If `samples` is None return a list of all the samples
    implied by `groups`. Otherwise return the samples in `samples`.

    Return everything as DataSuper records, not strings.
    '''

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
    return samples, groups


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
    elif type(obj) == int:
        return str(obj)
    else:
        print(type(obj), file=sys.stderr)
        print(obj, file=sys.stderr)
        assert False  # panic
