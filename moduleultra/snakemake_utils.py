from .utils import joinResultNameType
import datasuper as ds


def getSample(resultFilename):
    return resultFilename.split('/')[-1].split('.')[0]


def getOriginResultFiles(config, resultType, fileType):
    '''
    N.B. This function returns another function!
    It does not return the filepath itself
    '''

    def getter(wcs):
        try:
            return config['origins'][resultType][wcs.sample_name][fileType]
        except AttributeError:
            return config['origins'][resultType][wcs.group_name][fileType]

    return getter


def expandGroup(*samplePatterns):
    '''
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
                patterns.append(p)
        return patterns

    return getter

