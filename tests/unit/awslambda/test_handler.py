#!/usr/bin/env python
# -*- coding: utf-8 -*-
# pylint: disable=unused-argument
"""Test base objects."""
import os
from boto3 import client
from moto import mock_secretsmanager
from edfred.oob.awslambda.handler import SQLHandler
import mock
import pymysql


@mock_secretsmanager
def test_sql_handler():
    os.environ["SCHEMA_NAME"] = "test"
    os.environ["ACCOUNT_ID"] = "123456789012"
    os.environ["CLUSTER_JDBC"] = "jdbc:postgresql://rds-cluster-id.cavpiws4jltu.eu-west-1.rds.amazonaws.com:5432/adm"
    os.environ[
        "SECRET_CREDENTIALS_ARN"
    ] = "arn:aws:secretsmanager:eu-west-1:123456789012:secret:rds-db-credentials/master-Hc2JJi"
    sm = client("secretsmanager")
    sm.create_secret(
        Name=os.environ["SECRET_CREDENTIALS_ARN"], SecretString='[{"Username":"bob"},{"Password":"abc123"}]',
    )
    with mock.patch("edfred.oob.rds.mysql.Connection") as connect_mock:
        connect_mock.return_value = mock.Mock()
        handler = SQLHandler()
        assert handler.schema_name == os.environ.get("SCHEMA_NAME")
        assert handler.cluster_jdbc == os.environ.get("CLUSTER_JDBC")
        assert handler.account_id == os.environ.get("ACCOUNT_ID")
        assert handler.cluster_arn == "arn:aws:rds:eu-west-1:123456789012:cluster:rds-cluster-id"
        handler(
            {
                "ManualCall": {
                    "Environ": {
                        "CLUSTER_JDBC": "jdbc:postgresql://rds-cluster-id.cavpiws4jltu.eu-west-1.rds.amazonaws.com:5432/test"
                    }
                }
            },
            {},
        )
        assert (
            handler.environ["CLUSTER_JDBC"]
            == "jdbc:postgresql://rds-cluster-id.cavpiws4jltu.eu-west-1.rds.amazonaws.com:5432/test"
        )
        assert (
            handler.cluster_jdbc
            == "jdbc:postgresql://rds-cluster-id.cavpiws4jltu.eu-west-1.rds.amazonaws.com:5432/test"
        )
