#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='cassandra_snapshotter',
    version='0.0.1',
    install_requires=['pyyaml'],
    find_packages=find_packages()
)
