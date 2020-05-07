from aurora_data_api import AuroraDataAPIClient
from .extentions import Base, MySQL


class Connection(AuroraDataAPIClient):
    def __init__(self, *args, **kwargs):
        super().__init__(
            dbname=kwargs["db"],
            aurora_cluster_arn=kwargs["aurora_cluster_arn"],
            secret_arn=kwargs["secret_arn"],
            rds_data_client=kwargs.get("rds_data_client", None),
            charset=kwargs.get("charset", None),
        )


class MySQLConnection(Connection, Base, MySQL):
    pass
