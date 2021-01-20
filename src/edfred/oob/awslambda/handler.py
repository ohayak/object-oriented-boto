import os
import inspect
from typing import ClassVar, List, Generic
from dataclasses import dataclass, field, asdict, InitVar
from . import Handler, Event
from edfred.oob.secretsmanager import SecretValue
from edfred.oob.rds import AWSJdbc, connect


@dataclass
class SQLHandler(Handler):
    dbtype: str = "mysql"
    use_data_api: bool = False
    conn: object = None
    schema_name: str = field(init=False)
    cluster_jdbc: str = field(init=False)
    account_id: str = field(init=False)
    jdbc: str = field(init=False)
    credentials: str = field(init=False)
    cluster_arn: str = field(init=False)

    def __post_init__(self):
        """Initialize the handler."""
        Handler.__post_init__(self)
        if self.dbtype == "athena" and self.conn is None:
            raise KeyError("For Athena connection, provide conn parameter (all other parameters are ignored).")
        elif self.dbtype == "custom":
            if self.conn is None:
                raise KeyError("For custom connection conn parameter cannot be null.")
        else:
            self.schema_name = self.environ.get("SCHEMA_NAME")
            self.cluster_jdbc = self.environ.get("CLUSTER_JDBC")
            self.jdbc = AWSJdbc(self.cluster_jdbc)
            self.credentials = SecretValue(self.environ.get("SECRET_CREDENTIALS_ARN"))
            self.cluster_arn = f"arn:aws:rds:{self.jdbc.region}:{self.account_id}:cluster:{self.jdbc.identifier}"
            conn_args = {
                "user": self.credentials.attributes.username,
                "password": self.credentials.attributes.password,
                "host": self.jdbc.host,
                "port": int(self.jdbc.port),
                "database": self.jdbc.database,
            }
            if self.use_data_api:
                conn_args["aurora_cluster_arn"] = self.cluster_arn
                conn_args["secret_arn"] = self.credentials.secret_id
            self.conn = connect(self.dbtype, use_data_api=self.use_data_api, **conn_args)

    def perform(self, event: Event):
        pass
