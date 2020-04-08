import re
import logging
from time import sleep
from dataclasses import dataclass, field


def connect(dbtype, **kwargs):
    if dbtype not in ["mysql", "pgsql", "redshift"]:
        raise KeyError(f"dbtype={dbtype} must be `mysql` or `pgsql` or `redshift`")

    if dbtype in ["pgsql", "redshift"]:
        if "db" in kwargs:
            kwargs["dbname"] = kwargs["db"]
            del kwargs["db"]
        if "database" in kwargs:
            kwargs["dbname"] = kwargs["database"]
            del kwargs["database"]
        from .pgsql import Connection

    elif dbtype == "mysql":
        if "dbname" in kwargs:
            kwargs["db"] = kwargs["dbname"]
            del kwargs["dbname"]
        if "database" in kwargs:
            kwargs["db"] = kwargs["database"]
            del kwargs["database"]
        from .mysql import Connection

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


class Extentions(object):
    def execute_transaction(self, statements, max_retries=0, retry_delay=10, logger=None, exception_cls=Exception):
        log = logger if logger else logging
        for i in range(max_retries + 1):
            try:
                log.info("Start SQL transaction")
                with self.cursor() as cur:
                    for stm in statements:
                        log.debug(stm)
                        cur.execute(stm)
                self.commit()
                log.info("End SQL transaction.")
                return
            except exception_cls as e:
                self.rollback()
                if i < max_retries:
                    log.warning(f"Error raised: {e}, Rollback and retry.")
                    log.warning(f"Retry-{i+1} after {retry_delay} secs")
                    sleep(retry_delay)
                    continue
                log.error(f"Retries maxout. Rollback and Raise error")
                log.error(e)
                raise e

    def get_list_tables(self, schema_name):
        list_tables = []
        with self.cursor() as cur:
            cur.execute(f"select table_name from information_schema.tables where table_schema = '{schema_name}';")
            result = cur.fetchall()
            for record in result:
                list_tables.append(record[0])
        return list_tables

    def get_table_columns_list(self, schema_name, table_name):
        list_columns = []
        with self.cursor() as cur:
            cur.execute(
                f"select column_name from information_schema.columns "
                f"where table_schema = '{schema_name}' "
                f"and table_name = '{table_name}' "
                f"order by ordinal_position;"
            )
            result = cur.fetchall()
            for record in result:
                list_columns.append(record[0])
        self.commit()
        return list_columns

    @staticmethod
    def parse_sql_file(filename):
        with open(filename, "r") as f:
            data = f.readlines()
            stmts = []
            DELIMITER = ";"
            stmt = ""
            for lineno, line in enumerate(data):
                if not line.strip():
                    continue
                if line.startswith("--"):
                    continue
                if "DELIMITER" in line:
                    DELIMITER = line.split()[1]
                    continue
                if DELIMITER not in line:
                    stmt += line.replace(DELIMITER, ";")
                    continue
                if stmt:
                    stmt += line
                    stmts.append(stmt.strip())
                    stmt = ""
                else:
                    stmts.append(line.strip())
            return stmts
