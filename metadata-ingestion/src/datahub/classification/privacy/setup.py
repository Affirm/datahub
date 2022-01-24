#!/usr/bin/env python
# ~*~ encoding: utf-8 ~*~
# @generated
"""setup.py for affirm.security.privacy"""
from __future__ import absolute_import
import os
from codecs import open

from setuptools import setup, find_packages


here = os.path.abspath(os.path.dirname(__file__))


PROJECT = 'privacy'
VERSION = '1.0.1'
URL = 'https://github.com/Affirm/privacy'
AUTHOR = 'Security'
AUTHOR_EMAIL = 'jiarui.xu@affirm.com'
DESC = "Privacy API"


def read_file(file_name):
    file_path = os.path.join(here, file_name)
    return open(file_path, encoding='utf-8').read().strip()


def parse_requirements(file_name):
    with open(os.path.join(here, file_name), encoding='utf-8') as f:
        return [x for x in f if len(x) > 0 and x[0] not in ('-', '#')]


setup(
    name=PROJECT,
    version=VERSION,
    description=DESC,
    long_description=read_file('README.md'),
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    keywords="",
    url=URL,
    license=read_file('LICENSE'),
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests", "doc"]),
    package_data={'conf': ['*.py'], '.': ['requirements*.txt']},
    include_package_data=True,
    zip_safe=False,
    install_requires=parse_requirements("requirements.txt"),
    entry_points={'console_scripts': []},
    classifiers=["Programming Language :: Python"],
)
