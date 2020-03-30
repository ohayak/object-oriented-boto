from edfred.oob.messaging.sns import SNSTopic
from edfred.oob.utils import underscore_namedtuple
from boto3 import client
from moto import mock_sns
import os


@mock_sns
def test_sqs_topic():
    sns = client("sns")
    topic_arn = sns.create_topic(Name="sns-lambda")["TopicArn"]
    topic = SNSTopic(arn=topic_arn, subject="test")
    topic.publish("Hello")
