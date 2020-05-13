import psycopg2
from .extentions import Base, PgSQL


class Connection(psycopg2.extensions.connection, Base, PgSQL):
    pass
