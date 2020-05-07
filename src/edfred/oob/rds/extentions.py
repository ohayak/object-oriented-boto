import logging
from time import sleep
from edfred.oob.s3 import S3Object


class Base:
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
                    log.warning(f"Try to execute: {stm} but error raised: {e}, Rollback and retry.")
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


class MySQL:
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
        split_lines=False,
    ):
        s3_url = f"s3://{s3object.bucket_name}/{s3object.key}"
        fileobj = s3object.download_fileobj()
        columns = fileobj.readline().decode("utf-8-sig").rstrip().split(fields_delimiter)
        stmt = None
        if split_lines:
            stmt = []
            for line in fileobj.read().splitlines():
                values = line.decode("utf-8-sig").split(fields_delimiter)
                if sum(map(len, values)) == 0:
                    continue
                values_string = "'" + "', '".join(values) + "'"
                columns_string = ", ".join(columns)
                updates = []
                for k, v in zip(columns, values):
                    updates.append(f"{k} = '{v}'")
                updates_string = ", ".join(updates)

                stmt.append(
                    f"INSERT INTO {schema}.{table} ({columns_string}) "
                    f"VALUES ({values_string}) "
                    f"ON DUPLICATE KEY UPDATE {updates_string};"
                )
        else:
            stmt = (
                f"LOAD DATA FROM S3 '{s3_url}' {action} "
                f"INTO TABLE {schema}.{table} "
                f"character set {encoding} "
                f"fields terminated by '{fields_delimiter}' "
                f"lines terminated by '{lines_delimiter}' "
                f"ignore {ignore_lines} LINES "
                f"({','.join(columns)});"
            )
        fileobj.close()
        return stmt

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
