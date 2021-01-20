#!/usr/bin/env python
# -*- coding: utf-8 -*-
# pylint: disable=unused-argument
"""Test base objects."""
from edfred.oob.awslambda.event import *
from boto3 import client
from moto import mock_sqs, mock_sns


@mock_sqs
def test_sqs_event():
    """Test SQS Event."""
    event = {
        "Records": [
            {
                "messageId": "059f36b4-87a3-44ab-83d2-661975830a7d",
                "receiptHandle": "AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a...",
                "body": "Test message.",
                "attributes": {
                    "ApproximateReceiveCount": "1",
                    "SentTimestamp": "1545082649183",
                    "SenderId": "AIDAIENQZJOLO23YVJ4VO",
                    "ApproximateFirstReceiveTimestamp": "1545082649185",
                },
                "messageAttributes": {},
                "md5OfBody": "e4e68fb7bd0e697a0ae8f1bb342846b3",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:eu-west-1:123456789012:my-queue",
                "awsRegion": "eu-west-1",
            }
        ]
    }
    sqs = client("sqs", region_name="eu-west-1")
    sqs.create_queue(QueueName="my-queue")
    event = SQSEvent()(event, {})
    assert event.message.body == "Test message."
    assert event.message.body_md5 == "e4e68fb7bd0e697a0ae8f1bb342846b3"
    assert event.message.region == "eu-west-1"
    assert event.message.attributes._asdict() == {
        "approximate_receive_count": "1",
        "sent_timestamp": "1545082649183",
        "sender_id": "AIDAIENQZJOLO23YVJ4VO",
        "approximate_first_receive_timestamp": "1545082649185",
    }
    assert event.message.message_attributes == {}
    assert event.message.id == "059f36b4-87a3-44ab-83d2-661975830a7d"
    assert event.message.receipt_handle == "AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a..."


@mock_sns
def test_sns_event():
    sns = client("sns")
    topic_arn = sns.create_topic(Name="sns-lambda")["TopicArn"]
    sub_arn = sns.subscribe(TopicArn=topic_arn, Protocol="lambda", ReturnSubscriptionArn=True)["SubscriptionArn"]
    payload = {
        "Records": [
            {
                "EventVersion": "1.0",
                "EventSubscriptionArn": sub_arn,
                "EventSource": "aws:sns",
                "Sns": {
                    "SignatureVersion": "1",
                    "Timestamp": "2019-01-02T12:45:07.000Z",
                    "Signature": "tcc6faL2yUC6dgZdmrwh1Y4cGa/ebXEkAi6RibDsvpi+tE/1+82j...65r==",
                    "SigningCertUrl": "https://sns.us-east-2.amazonaws.com/SimpleNotificationService-ac565b8b1a6c5d002d285f9598aa1d9b.pem",
                    "MessageId": "95df01b4-ee98-5cb9-9903-4c221d41eb5e",
                    "Message": "Hello from SNS!",
                    "MessageAttributes": {
                        "Test": {"Type": "String", "Value": "TestString"},
                        "TestBinary": {"Type": "Binary", "Value": "TestBinary"},
                    },
                    "Type": "Notification",
                    "UnsubscribeUrl": "https://sns.us-east-2.amazonaws.com/?Action=Unsubscribe&amp;SubscriptionArn=arn:aws:sns:us-east-2:123456789012:test-lambda:21be56ed-a058-49f5-8c98-aedd2564c486",
                    "TopicArn": topic_arn,
                    "Subject": "TestInvoke",
                },
            }
        ]
    }

    event = SNSEvent()(payload, {})
    assert event.notification.message_id == "95df01b4-ee98-5cb9-9903-4c221d41eb5e"


def test_sfn_event():
    """Test SFN Event."""
    payload = {"georefOf": "Home", "coords": {"carts": {"x-datum": 0.381018, "y-datum": 622.2269926397355}}}
    event = SFNEvent("coords.carts")(payload, {})
    assert event.result == {"x-datum": 0.381018, "y-datum": 622.2269926397355}
    assert event.payload["georefOf"] == "Home"


def test_apig_event():
    """Test APIG Event."""
    event = APIGatewayEvent()(
        {
            "body": "hello",
            "headers": {"X-Client": "APIG-EVENT-TEST"},
            "pathParameters": {"user-id": "user-1234"},
            "queryStringParameters": {"filter-x": "asc"},
        },
        {},
    )
    assert event.body == "hello"
    assert event.headers.x_client == "APIG-EVENT-TEST"
    assert event.path.user_id == "user-1234"
    assert event.query.filter_x == "asc"
    assert event.context == {}
