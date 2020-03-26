#!/usr/bin/env python
import os
import re
from dataclasses import dataclass, field
from collections import namedtuple
import time
import zipfile
from datetime import datetime
from inflection import underscore


@dataclass
class AWSJdbc:
    url: str
    awsurl: str = field(init=False, default=None)
    engine: str = field(init=False, default=None)
    identifier: str = field(init=False, default=None)
    account_adress: str = field(init=False, default=None)
    region: str = field(init=False, default=None)
    aws_service: str = field(init=False, default=None)
    port: str = field(init=False, default=None)
    dbname: str = field(init=False, default=None)

    def __post_init__(self):
        regex = r"jdbc:(\w+)://(([\w\d-]+).([\w\d]+).([\w\d-]+).([\w\d-]+).amazonaws.com):(\d+)/([\w\d-]+)"
        match = re.match(regex, self.url)
        self.awsurl = match.group(0)
        self.engine = match.group(1)
        self.host = match.group(2)
        self.identifier = match.group(3)
        self.account_adress = match.group(4)
        self.region = match.group(5)
        self.aws_service = match.group(6)
        self.port = match.group(7)
        self.dbname = match.group(8)


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


def archive(src, dest, filename):
    output = os.path.join(dest, filename)
    zfh = zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED)

    for root, _, files in os.walk(src):
        for file in files:
            zfh.write(os.path.join(root, file))
    zfh.close()
    return os.path.join(dest, filename)


def timestamp(fmt="%Y-%m-%d-%H%M%S"):
    now = datetime.utcnow()
    return now.strftime(fmt)
