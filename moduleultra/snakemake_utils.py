import datasuper as ds
from os.path import isfile


def inputsToAllRule(config):
    '''Return a function thats lists all final inputs based on a config.'''
    def aller(wcs):
        out = []
        for sampleName in config['samples'].keys():
            for pattern in config['final_inputs']['sample_patterns']:
                out.append(pattern.format(sample_name=sampleName))
        for groupName in config['groups'].keys():
            for pattern in config['final_inputs']['group_patterns']:
                out.append(pattern.format(group_name=groupName))
        out = [f for f in out if not isfile(f)]
        return out
    return aller


def getSample(resultFilename):
    '''Return the samplename based off a result filename.'''
    return resultFilename.split('/')[-1].split('.')[0]


def getOriginResultFiles(config, resultType, fileType):
    '''Return a function that will in turn return paths to origins files.

    N.B. This function returns another function!
    It does not return the filepath itself
    '''

    def getter(wcs):
        try:
            return config['origins'][resultType][wcs.sample_name][fileType]
        except AttributeError:
            return config['origins'][resultType][wcs.group_name][fileType]

    return getter


def expandGroup(*samplePatterns, names=False):
    '''Return a function that returns all samples in a group.

    If `names` is True return a list of sample name strings.

    N.B. This function returns another function!
    It does not return the filepaths themselves
    '''

    def getter(wcs):
        gname = wcs.group_name
        dsrepo = ds.Repo.loadRepo()
        group = dsrepo.db.sampleGroupTable.get(gname)

        patterns = []
        for sample in group.allSamples():
            for pattern in samplePatterns:
                p = pattern.format(sample_name=sample.name)
                if names:
                    patterns.append(sample.name)
                else:
                    patterns.append(p)
        return patterns

    return getter
