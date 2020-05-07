from time import sleep
import pymysql
from .extentions import Base, MySQL


class Connection(pymysql.connections.Connection, Base, MySQL):
    pass
