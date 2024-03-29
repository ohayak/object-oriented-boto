import re
from dataclasses import dataclass, field


def connect(dbtype, use_data_api=False, **kwargs):
    supported_dbtype = ["mysql", "pgsql", "redshift", "athena"]
    if dbtype not in supported_dbtype:
        raise KeyError(f"dbtype must be one of {supported_dbtype}")

    if dbtype in ["pgsql", "redshift"]:
        if "db" in kwargs:
            kwargs["dbname"] = kwargs.pop("db")
        if "database" in kwargs:
            kwargs["dbname"] = kwargs.pop("database")
        if use_data_api:
            from .data_api import PgSQLConnection as Connection
        else:
            from .pgsql import Connection

    elif dbtype == "mysql":
        if "dbname" in kwargs:
            kwargs["db"] = kwargs.pop("dbname")
        if "database" in kwargs:
            kwargs["db"] = kwargs.pop("database")
        if use_data_api:
            from .data_api import MySQLConnection as Connection
        else:
            from .mysql import Connection

    elif dbtype == "athena":
        from .athena import Connection

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
    database: str = field(init=False, default=None)

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
        self.database = match.group(8)
