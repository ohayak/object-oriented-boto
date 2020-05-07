from edfred.oob.s3 import S3Object
import psycopg2
from .extentions import Base


class Connection(psycopg2.extensions.connection, Base):
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
