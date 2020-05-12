import psycopg2
from edfred.oob.s3 import S3Object
from .extentions import Base


class Connection(psycopg2.extensions.connection, Base):
    pass
