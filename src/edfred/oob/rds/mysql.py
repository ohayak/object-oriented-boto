from edfred.oob.s3 import S3Object
from time import sleep
import logging
import pymysql
from . import Extentions


class Connection(pymysql.connections.Connection, Extentions):
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


class __MgSQLConnection:
    import psycopg2

    class Connection(psycopg2.extensions.connection):
        pass


class PgSQLConnection(__MgSQLConnection.Connection, Extentions):
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
