from pymysql.connections import Connection
from psycopg2.extensions import connection
from edfred.oob.s3 import S3Object
from time import sleep
import logging


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
        return PgSQLConnection(**kwargs)
    elif dbtype == "mysql":
        if "dbname" in kwargs:
            kwargs["db"] = kwargs["dbname"]
            del kwargs["dbname"]
        if "database" in kwargs:
            kwargs["db"] = kwargs["database"]
            del kwargs["database"]
        return MySQLConnection(**kwargs)


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
                if i < max_retries:
                    log.warning(f"Error raised: {e}, Rollback and retry.")
                    self.rollback()
                    log.warning(f"Retry-{i+1} after {retry_delay} secs")
                    sleep(retry_delay)
                    continue
                log.error(f"Retries maxout. Raise error")
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


class MySQLConnection(Connection, Extentions):
    @staticmethod
    def format_s3_load_statement(
        s3object: S3Object,
        schema,
        table,
        ignore_lines=1,
        fields_delimiter=";",
        lines_delimiter="\n",
        action="ignore",
        encoding="utf8",
    ):
        s3_url = f"s3://{s3object.bucket_name}/{s3object.key}"
        fileobj = s3object.download_fileobj()
        columns = fileobj.readline().decode("utf-8-sig").split(fields_delimiter)
        fileobj.close()
        statement = (
            f"LOAD DATA FROM S3 '{s3_url}' {action} "
            f"INTO TABLE {schema}.{table} "
            f"character set {encoding} "
            f"fields terminated by '{fields_delimiter}' "
            f"lines terminated by '{lines_delimiter}' "
            f"ignore {ignore_lines} LINES "
            f"({','.join(columns)});"
        )
        return statement

    def load_from_s3(
        self,
        s3object: S3Object,
        schema,
        table,
        ignore_lines=1,
        fields_delimiter=";",
        lines_delimiter="\n",
        action="ignore",
        encoding="utf8",
    ):
        with self.cursor() as cur:
            cur.execute(
                self.format_s3_load_statement(
                    s3object,
                    schema,
                    table,
                    ignore_lines=1,
                    fields_delimiter=";",
                    lines_delimiter="\n",
                    action="ignore",
                    encoding="utf8",
                )
            )
        self.commit()

    @staticmethod
    def format_s3_unload_statement(
        bucket, key, schema, table, fields_delimiter=";", lines_delimiter="\n", encoding="utf8"
    ):
        s3_url = f"s3://{bucket}/{key}"
        statement = (
            f"SELECT * FROM {schema}.{table} INTO OUTFILE S3 '{s3_url}' "
            f"character set {encoding} "
            f"fields terminated by '{fields_delimiter}' "
            f"lines terminated by '{lines_delimiter}' "
            f"overwrite on;"
        )
        return statement

    def load_into_s3(self, bucket, key, schema, table, fields_delimiter=";", lines_delimiter="\n", encoding="utf8"):
        with self.cursor() as cur:
            cur.execute(
                self.format_s3_unload_statement(
                    bucket, key, schema, table, fields_delimiter=";", lines_delimiter="\n", encoding="utf8"
                )
            )
        self.commit()

    @staticmethod
    def format_local_load_statement(
        filepath,
        schema,
        table,
        ignore_lines=1,
        fields_delimiter=";",
        lines_delimiter="\n",
        action="ignore",
        encoding="utf8",
    ):
        f = open(filepath, "r")
        columns = f.readline().split(fields_delimiter)
        f.close()
        statement = (
            f"LOAD DATA LOCAL INFILE '{filepath}' {action} "
            f"INTO TABLE {schema}.{table} "
            f"character set {encoding} "
            f"fields terminated by '{fields_delimiter}' "
            f"lines terminated by '{lines_delimiter}' "
            f"ignore {ignore_lines} LINES "
            f"({','.join(columns)});"
        )
        return statement

    def get_create_table_query(self, schema, table):
        with self.cursor() as cur:
            cur.execute(f"SHOW CREATE TABLE {schema}.{table};")
            result = cur.fetchall()
        self.commit()
        return result[0][1] + ";"


class PgSQLConnection(connection, Extentions):
    @staticmethod
    def format_s3_load_statement(s3object: S3Object, schema, table, region, delimiter=";"):
        return f"select aws_s3.table_import_from_s3('\"{schema}\".\"{table}\"', '', '(format csv, header true, delimiter '{delimiter}')', '{s3object.bucket_name}', '{s3object.key}', '{region}');"

    def get_create_table_query(self, schema, table):
        with self.cursor() as cur:
            # show_create_table sql function
            statement = """
                CREATE OR REPLACE FUNCTION show_create_table(table_schema text, table_name text, join_char text = E'\n' ) 
                RETURNS text AS 
                $$
                SELECT 'CREATE TABLE ' || $2 || ' (' || $3 || '' || 
                    string_agg(column_list.column_expr, ', ' || $3 || '') || 
                    '' || $3 || ');'
                FROM (
                SELECT '    ' || column_name || ' ' || data_type || 
                    coalesce('(' || character_maximum_length || ')', '') || 
                    case when is_nullable = 'NO' then ' NOT NULL' else '' end ||
                        case when column_default is not NULL then ' DEFAULT ' || column_default else '' end
                        as column_expr
                FROM information_schema.columns AS table_info
                WHERE table_info.table_schema = $1 AND table_info.table_name = $2
                ORDER BY ordinal_position) column_list;
                $$
                LANGUAGE SQL STABLE;
            """
            cur.execute(statement)
            cur.execute(f"SELECT show_create_table('{schema}', '{table}');")
            result = cur.fetchone()[0]
        self.commit()
        return result
