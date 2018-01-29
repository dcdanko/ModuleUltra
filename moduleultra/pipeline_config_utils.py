from subprocess import check_output, CalledProcessError
from inspect import stack
from os.path import dirname, join
from sys import stderr


def resolveCmd(cmd):
    try:
        cmdOut = check_output(cmd, shell=True)
        out = cmdOut.decode('utf-8').strip()
    except CalledProcessError:
        print('subcommand "{}" failed'.format(cmd),
              file=stderr)
        out = ''
    return out


def fromPipelineDir(fpath):
    '''
    Works by assuming this function is being called
    directly from the conf. Could be pretty unstable
    but cool if it works.
    '''
    callingframe = stack()[1]
    fname = callingframe.filename
    dname = dirname(fname)
    return join(dname, fpath)
