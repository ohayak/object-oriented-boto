#!/usr/bin/env python
# -*- coding: utf-8 -*-
# pylint: disable=unused-argument
"""Test base objects."""
from edfred.oob.awslambda.event import S3Event, SQSEvent, APIGatewayEvent, SFNEvent
from boto3 import client
from moto import mock_sqs


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
