import os
import inspect
from typing import ClassVar, List, Generic
from dataclasses import dataclass, field, asdict, InitVar
from . import Handler, Event
from edfred.oob.secretsmanager import SecretValue
from edfred.oob.utils import AWSJdbc, sql


@dataclass
class SQLHandler(Handler):
    schema_name: str = field(init=False)
    cluster_jdbc: str = field(init=False)
    account_id: str = field(init=False)
    jdbc: str = field(init=False)
    credentials: str = field(init=False)
    cluster_arn: str = field(init=False)
    conn: object = field(init=False)
    connector: InitVar = None

    def __post_init__(self, connector):
        """Initialize the handler."""
        super().__post_init__()
        self.schema_name = os.environ.get("SCHEMA_NAME")
        self.cluster_jdbc = os.environ.get("CLUSTER_JDBC")
        self.account_id = os.environ.get("ACCOUNT_ID")
        self.jdbc = AWSJdbc(self.cluster_jdbc)
        self.credentials = SecretValue(os.environ.get("SECRET_CREDENTIALS_ARN"))
        self.cluster_arn = f"arn:aws:rds:{self.jdbc.region}:{self.account_id}:cluster:{self.jdbc.identifier}"
        conn_args = {
            "user": self.credentials.attributes.username,
            "password": self.credentials.attributes.password,
            "host": self.jdbc.host,
            "port": int(self.jdbc.port),
        }
        if "dbname" in inspect.getfullargspec(connector.connect)[0]:
            conn_args["dbname"] = self.jdbc.dbname
        else:
            conn_args["db"] = self.jdbc.dbname
        self.conn = connector.connect(**conn_args)
