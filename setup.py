#!/usr/bin/env python


import os
import re
from distutils.core import setup
from setuptools import setup, find_packages


def parse_requirements(file_name):
    requirements = []
    for line in open(file_name, 'r').read().split('\n'):
        if re.match(r'(\s*#)|(\s*$)', line):
            continue
        if re.match(r'\s*-e\s+', line):
            requirements.append(re.sub(r'\s*-e\s+.*#egg=(.*)$', r'\1', line))
        elif re.match(r'\s*-f\s+', line):
            pass
        else:
            requirements.append(line)

    return requirements


def parse_dependency_links(file_name):
    dependency_links = []
    for line in open(file_name, 'r').read().split('\n'):
        if re.match(r'\s*-[ef]\s+', line):
            dependency_links.append(re.sub(r'\s*-[ef]\s+', '', line))

    return dependency_links


# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))


setup(
  name='pyleveltsd',
  version='0.0.2',
  url='https://github.com/inmobi/level-tsd',
  author='InMobi',
  author_email='nobody@inmobi.com',
  license='Apache Software License 2.0',
  description='leveldb backend for carbon',
  packages=['pyleveltsd',],
  install_requires=parse_requirements('requirements.txt'),
  dependency_links=parse_dependency_links('requirements.txt'),
)
