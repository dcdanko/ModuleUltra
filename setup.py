"""ModuleUltra -- biological data pipeline creation."""

import os
import sys

from setuptools import setup, find_packages


version = {}
VERSION_PATH = os.path.dirname(os.path.realpath(__file__))
with open('{0}/moduleultra/version.py'.format(VERSION_PATH)) as fp:
    exec(fp.read(), version)


with open('README.rst') as readme_file:
    readme = readme_file.read()


requirements = [
    'click==6.7',
    'snakemake==4.1.0',
    'yaml-backed-structs==0.9.0',
    'datasuper==0.10.0',
    'gimme_input==0.1.0',
    'PackageMega==0.1.0',
]


dependency_links = [
    'git+https://github.com/dcdanko/gimme_input.git#egg=gimme_input-0.1.0',
    'git+https://github.com/dcdanko/PackageMega.git#egg=PackageMega-0.1.0',
]


setup(
    name='ModuleUltra',
    version=version['__version__'],
    url='https://github.com/dcdanko/ModuleUltra',

    author=version['__author__'],
    author_email=version['__email__'],

    description=('Tools to make pipelines easier to run and distribute for '
                 'large biological datasets'),
    long_description=readme,

    packages=packages=find_packages(include=['moduleultra']),
    install_requires=requirements,

    entry_points={
        'console_scripts': [
            'moduleultra=moduleultra.cli:main',
        ],
    },

    classifiers=[
        # As from http://pypi.python.org/pypi?%3Aaction=list_classifiers
        'Development Status :: 2 - Pre-Alpha',
        'Programming Language :: Python :: 3.6',
    ],
    cmdclass={
        'verify': VerifyVersionCommand,
    },
)
