#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Setup."""

# Note: To use the 'upload' functionality of this file, you must:
#   $ pip install twine

import os
import sys
from shutil import rmtree

from setuptools import Command, find_namespace_packages, setup

# ----------------------------------------------------------------------------
# Helper Functions
# ----------------------------------------------------------------------------

ROOT = os.path.dirname(__file__)
VERSION = "0.0.4.DEV0"

def file_contents(file_name):
    """Given a file name to a valid file returns the file object."""
    curr_dir = os.path.abspath(os.path.dirname(__file__))
    with open(os.path.join(curr_dir, file_name)) as the_file:
        contents = the_file.read()
    return contents

# ----------------------------------------------------------------------------
# Setup
# ----------------------------------------------------------------------------

setup(
    name="edfred-oob-library",
    version=VERSION,
    description="Object Oriented Boto3 library",
    long_description=file_contents("README.rst"),
    long_description_content_type='text/rst',
    author="Omar HAYAK",
    author_email="ohayak@quantmetry.com",
    python_requires='>=3.6.0',
    url="https://git-codecommit.eu-west-1.amazonaws.com/v1/repos/edfred-oob-library",    
    package_dir={'': 'src'},
    packages=find_namespace_packages(where='src', include=['edfred.*']),
    install_requires=[
        'typing',
        'dataclasses',
        'inflection',
        'namedtupled',
        'autologging'
    ],
    extras_require={
        'occasional': ['boto3', 'pandas', 'pymysql', 'psycopg2-binary'],
        'test': [
            'pymysql', 
            'psycopg2-binary',
            'moto',
            'pytest',
            'pytest-html',
            'pytest-mock',
            'pytest-cov', 
        ]
    },
    include_package_data=True,
    license='MIT',
    classifiers=[
        # Trove classifiers
        # Full list: https://pypi.python.org/pypi?%3Aaction=list_classifiers
        'License :: OSI Approved :: GNU General Public License v2 (GPLv2)',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy'
    ]
)