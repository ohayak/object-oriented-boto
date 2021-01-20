import pytest
import moto
import boto3


@pytest.fixture(scope="session")
def s3():
    with moto.mock_s3():
        yield boto3.client("s3")


@pytest.fixture(scope="session")
def sts():
    with moto.mock_sts():
        yield boto3.client("sts")


@pytest.fixture(scope="session")
def cloudwatch():
    with moto.mock_cloudwatch():
        yield boto3.client("cloudwatch")


@pytest.fixture(scope="session")
def secretmanager():
    with moto.mock_secretsmanager():
        yield boto3.client("secretmanager")


@pytest.fixture(scope="session")
def sqs():
    with moto.mock_sqs():
        yield boto3.client("sqs")


@pytest.fixture(scope="session")
def sns():
    with moto.mock_sqs():
        yield boto3.client("sns")
