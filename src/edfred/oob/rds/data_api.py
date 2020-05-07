import aurora_data_api
from .extentions import Base, MySQL


class Connection(aurora_data_api.AuroraDataAPIClient):
    def __init__(self, *args, **kwargs):
        super().__init__(
            dbname=kwargs["db"],
            aurora_cluster_arn=kwargs["aurora_cluster_arn"],
            secret_arn=kwargs["secret_arn"],
            rds_data_client=kwargs["rds_data_client"],
            charset=kwargs["charset"],
        )


class MySQLConnection(Connection, Base, MySQL):
    pass
