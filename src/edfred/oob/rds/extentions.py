import logging
from time import sleep
from edfred.oob.s3 import S3Object


class Base:
    @staticmethod
    def _parse_header(file_obj, file_encoding, fields_delimiter, columns):
        if not columns:
            file_obj.seek(0)
            columns = file_obj.readline().decode(file_encoding).split(fields_delimiter)
        return columns

    @staticmethod
    def _load_by_line_from_file_statement(
        file_obj, file_encoding, ignore_lines, columns, fields_delimiter, schema, table
    ):
        columns_string = ",".join(columns)
        statement = None
        stmt = []
        file_obj.seek(0)
        for i, line in enumerate(file_obj.read().splitlines(), start=1):
            if i <= ignore_lines:
                continue
            values = line.decode(file_encoding).split(fields_delimiter)
            if sum(map(len, values)) == 0:
                continue
            values_string = "','".join(values)

            updates_string = ""
            for k, v in zip(columns, values):
                updates_string = updates_string + "," + k + "=" + v

            statement.append(
                f"INSERT INTO {schema}.{table} ({columns_string}) "
                f"VALUES ('{values_string}') "
                f"ON DUPLICATE KEY UPDATE {updates_string};"
            )
        return stmt

    def execute_transaction(
        self, statements, max_retries=0, retry_delay=10, logger=None, exception_cls=Exception, **kwargs
    ):
        log = logger if logger else logging
        for i in range(max_retries + 1):
            try:
                log.info("Start SQL transaction")
                last_query = None
                with self.cursor() as cur:
                    for stm in statements:
                        last_query = stm
                        log.debug(stm)
                        cur.execute(stm, **kwargs)
                self.commit()
                log.info("End SQL transaction.")
                return
            except exception_cls as e:
                self.rollback()
                if i < max_retries:
                    log.warning(f"Try to execute: {last_query} but error raised: {e}, Rollback and retry.")
                    log.warning(f"Retry-{i+1} after {retry_delay} secs")
                    sleep(retry_delay)
                    continue
                log.error(f"Retries maxout. Rollback and Raise error")
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
    def load_from_s3_statement(
        s3object: S3Object,
        schema,
        table,
        file_encoding="utf-8-sig",
        ignore_lines=1,
        fields_delimiter=";",
        lines_delimiter="\n",
        action="ignore",
        encoding="utf8",
        split_lines=False,
        columns=None,
    ):
        s3_url = f"s3://{s3object.bucket_name}/{s3object.key}"
        file_obj = s3object.download_fileobj()
        columns = Base._parse_header(file_obj, file_encoding, fields_delimiter, columns)
        statement = None
        if split_lines:
            statement = Base._load_by_line_from_file_statement(
                file_obj, file_encoding, ignore_lines, columns, fields_delimiter, schema, table
            )
        else:
            statement = (
                f"LOAD DATA FROM S3 '{s3_url}' {action} "
                f"INTO TABLE {schema}.{table} "
                f"character set {encoding} "
                f"fields terminated by '{fields_delimiter}' "
                f"lines terminated by '{lines_delimiter}' "
                f"ignore {ignore_lines} LINES "
                f"({','.join(columns)});"
            )
        file_obj.close()
        return statement

    def load_from_s3(self, *args, **kwargs):
        with self.cursor() as cur:
            cur.execute(self.format_s3_load_statement(*args, **kwargs))
        self.commit()

    @staticmethod
    def load_into_s3_statement(
        bucket,
        key,
        schema,
        table,
        select_quey=None,
        fields_delimiter=";",
        lines_delimiter="\r\n",
        encoding="utf8",
        overwrite=True,
        format=None,
    ):
        s3_url = f"s3://{bucket}/{key}"
        if not select_quey:
            select_quey = f"SELECT * FROM {schema}.{table}"
        statement = (
            f"{select_quey} INTO OUTFILE S3 '{s3_url}' "
            f"character set {encoding} "
            f"{'format '+format if format else ''} "
            f"fields terminated by '{fields_delimiter}' "
            f"lines terminated by '{lines_delimiter}' "
            f"overwrite {'on' if overwrite else 'off'};"
        )
        return statement

    def load_into_s3(self, *args, **kwargs):
        with self.cursor() as cur:
            cur.execute(self.load_into_s3_statement(*args, **kwargs))
        self.commit()

    @staticmethod
    def load_from_file_statement(
        filepath,
        schema,
        table,
        file_encoding="utf-8",
        ignore_lines=1,
        fields_delimiter=";",
        lines_delimiter="\n",
        action="ignore",
        encoding="utf8",
        split_lines=False,
        columns=None,
    ):
        file_obj = open(filepath, "r")
        columns = Base._parse_header(file_obj, file_encoding, fields_delimiter, columns)
        statement = None
        if split_lines:
            statement = Base._load_by_line_from_file_statement(
                file_obj, file_encoding, ignore_lines, columns, fields_delimiter, schema, table
            )
        else:
            statement = (
                f"LOAD DATA LOCAL INFILE '{file_obj.name}' {action} "
                f"INTO TABLE {schema}.{table} "
                f"character set {encoding} "
                f"fields terminated by '{fields_delimiter}' "
                f"lines terminated by '{lines_delimiter}' "
                f"ignore {ignore_lines} LINES "
                f"({','.join(columns)});"
            )
        return statement

    def load_from_file(self, *args, **kwargs):
        with self.cursor() as cur:
            cur.execute(self.load_from_file_statement(*args, **kwargs))
        self.commit()

    def get_create_table_query(self, schema, table):
        with self.cursor() as cur:
            cur.execute(f"SHOW CREATE TABLE {schema}.{table};")
            result = cur.fetchall()
        self.commit()
        return result[0][1] + ";"


class PgSQL:
    @staticmethod
    def load_from_s3_statement(
        s3object: S3Object,
        schema,
        table,
        file_encoding="utf-8-sig",
        split_lines=False,
        columns=None,
        fields_delimiter=";",
        ignore_lines=1,
        encoding="utf8",
        extra_options={},
    ):
        s3_url = f"s3://{s3object.bucket_name}/{s3object.key}"
        file_obj = s3object.download_fileobj()
        columns = Base._parse_header(file_obj, file_encoding, fields_delimiter, columns)
        statement = None
        if split_lines:
            statement = Base._load_by_line_from_file_statement(
                file_obj, file_encoding, ignore_lines, columns, fields_delimiter, schema, table
            )
        else:
            options = dict(
                format="csv",
                encoding=encoding,
                header="true" if ignore_lines > 0 else "false",
                delimiter=fields_delimiter,
            )
            options.update(extra_options)
            options_string = ""
            for k, v in options.items():
                options_string = options_string + "," + k + " " + v
            statement = (
                f"select aws_s3.table_import_from_s3('\"{schema}\".\"{table}\"', '{','.join(columns)}',"
                f"'({options_string})',"
                f"'{s3object.bucket_name}', '{s3object.key}', '{s3object.region}');"
            )
        file_obj.close()
        return statement

    def load_from_s3(self, *args, **kwargs):
        with self.cursor() as cur:
            cur.execute(self.format_s3_load_statement(*args, **kwargs))
        self.commit()

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
            cur.execute(f"SELECT show_create_table('{schema}','{table}');")
            result = cur.fetchone()[0]
        self.commit()
        return result
