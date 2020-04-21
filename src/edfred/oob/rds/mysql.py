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
