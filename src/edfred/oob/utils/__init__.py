#!/usr/bin/env python
import os
import re
from dataclasses import dataclass, field
from collections import namedtuple
import time
import zipfile
from datetime import datetime
from inflection import underscore


def underscore_namedtuple(name, d):
    """Return dict as namedtuple."""
    payload = {underscore(k): v for k, v, in d.items()}
    dtuple = namedtuple(name, sorted(payload))
    the_tuple = dtuple(**payload)
    return the_tuple


def mkdir(path):
    if not os.path.exists(path):
        os.makedirs(path)


def read(path, loader=None, binary_file=False):
    open_mode = "rb" if binary_file else "r"
    with open(path, mode=open_mode) as fh:
        if not loader:
            return fh.read()
        return loader(fh.read())


def archive(src, dest):
    zfh = zipfile.ZipFile(dest, "w", zipfile.ZIP_DEFLATED)
    for root, _, files in os.walk(src):
        for file in files:
            zfh.write(os.path.join(root, file))
    zfh.close()
    return dest


def timestamp(fmt="%Y-%m-%d-%H%M%S"):
    now = datetime.utcnow()
    return now.strftime(fmt)


def timeit(f):
    """
    Timing function executions

    @timeit
    def my_function():
        ...
    """

    def timed(*args, **kw):
        ts = time.time()
        result = f(*args, **kw)
        te = time.time()
        print(f"Function: {f.__name__}")
        print(f"*  args: {args}")
        print(f"*  kw: {kw}")
        print(f"*  execution time: {(te-ts)*1000:8.2f} ms")
        return result

    return timed
