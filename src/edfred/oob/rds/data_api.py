from aurora_data_api import AuroraDataAPIClient, AuroraDataAPICursor, logger
import re
import reprlib
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
    
    def execute(self, operation, parameters=None, continue_after_timeout=False):
        self._current_response, self._iterator, self._paging_state = None, None, None
        execute_statement_args = dict(self._prepare_execute_args(operation),
                                      includeResultMetadata=True, continueAfterTimeout=continue_after_timeout)
        if parameters:
            execute_statement_args["parameters"] = self._format_parameter_set(parameters)
        logger.debug("execute %s", reprlib.repr(operation.strip()))
        try:
            res = self._client.execute_statement(**execute_statement_args)
            if "columnMetadata" in res:
                self._set_description(res["columnMetadata"])
            self._current_response = self._render_response(res)
        except self._client.exceptions.BadRequestException as e:
            if "Please paginate your query" in str(e):
                self._start_paginated_query(execute_statement_args)
            elif "Database response exceeded size limit" in str(e):
                self._start_paginated_query(execute_statement_args, records_per_page=max(1, self.arraysize // 2))
            else:
                raise self._get_database_error(e) from e
        self._iterator = iter(self)


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
