from edfred.oob.messaging.sns import SNSTopic, SNSTopicNotification
from edfred.oob.utils import underscore_namedtuple
from boto3 import client
from moto import mock_sns
import os


@mock_sns
def test_sns_topic():
    sns = client("sns")
    topic_arn = sns.create_topic(Name="sns-lambda")["TopicArn"]
    topic = SNSTopic(arn=topic_arn)
    topic.publish("Hello")


@mock_sns
def test_sns_topic_notification():
    sns = client("sns")
    topic_arn = sns.create_topic(Name="sns-lambda")["TopicArn"]
    notification = SNSTopicNotification(topic_arn=topic_arn, subject="test", message="Hello")
    notification.publish()
