from aurora_data_api import AuroraDataAPIClient, AuroraDataAPICursor
import re
from .extentions import Base, MySQL, PgSQL

class Cursor(AuroraDataAPICursor):

    def _prepare_execute_args(self, operation):
        """
        Named parameters are specified  with :name or %(name)s
        """
        args = re.finditer(r"\%\((.*?)\)s", operation)
        for arg in args:
            operation = operation.replace(arg.group(0), ":" + arg.group(1))
        return super()._prepare_execute_args(operation)


class Connection(AuroraDataAPIClient):
    def __init__(self, *args, **kwargs):
        super().__init__(
            dbname=kwargs["db"],
            aurora_cluster_arn=kwargs["aurora_cluster_arn"],
            secret_arn=kwargs["secret_arn"],
            rds_data_client=kwargs.get("rds_data_client", None),
            charset=kwargs.get("charset", None),
        )

    def cursor(self):
        cursor = super().cursor()
        cursor = Cursor(client=cursor._client,
                        dbname=cursor._dbname,
                        aurora_cluster_arn=cursor._aurora_cluster_arn,
                        secret_arn=cursor._secret_arn,
                        transaction_id=cursor._transaction_id)
        return cursor


class MySQLConnection(Connection, Base, MySQL):
    pass


class PgSQLConnection(Connection, Base, PgSQL):
    pass
