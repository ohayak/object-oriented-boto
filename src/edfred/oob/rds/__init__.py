import re
from dataclasses import dataclass, field


def connect(dbtype, use_data_api=False, **kwargs):
    if dbtype not in ["mysql", "pgsql", "redshift"]:
        raise KeyError(f"dbtype={dbtype} must be `mysql` or `pgsql` or `redshift`")

    if dbtype in ["pgsql", "redshift"]:
        if use_data_api:
            raise KeyError(f"AWS Aurora Serverless Data API do not support {dbtype}")
        if "db" in kwargs:
            kwargs["dbname"] = kwargs["db"]
            del kwargs["db"]
        if "database" in kwargs:
            kwargs["dbname"] = kwargs["database"]
            del kwargs["database"]
        from .pgsql import Connection

    elif dbtype == "mysql":
        if use_data_api:
            if not set(["aurora_cluster_arn", "secret_arn"]).issubset(kwargs.keys()):
                raise KeyError(f"aurora_cluster_arn and secret_arn arguments are required to use DATA API")
            from .data_api import MySQLConnection as Connection
        else:
            from .mysql import Connection
        if "dbname" in kwargs:
            kwargs["db"] = kwargs["dbname"]
            del kwargs["dbname"]
        if "database" in kwargs:
            kwargs["db"] = kwargs["database"]
            del kwargs["database"]

    return Connection(**kwargs)


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
        regex = r"jdbc:(\w+)://(([\w\d-]+).([\w\d-]+).([\w\d-]+).([\w\d-]+).amazonaws.com):(\d+)/([\w\d-]+)"
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
