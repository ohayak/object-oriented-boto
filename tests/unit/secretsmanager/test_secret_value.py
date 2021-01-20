from boto3 import client
from moto import mock_secretsmanager
from edfred.oob.secretsmanager import SecretValue


@mock_secretsmanager
def test_secret_manager():
    sm = client("secretsmanager")
    sm.create_secret(
        Name="my-secret", SecretString='[{"Username":"bob"},{"Password":"abc123"}]',
    )
    credentials = SecretValue("my-secret")
    assert credentials.secret_string == '[{"Username":"bob"},{"Password":"abc123"}]'
    assert credentials.attributes.username == "bob"
    assert credentials.attributes.password == "abc123"
