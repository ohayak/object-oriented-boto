#!/usr/bin/env python
# -*- coding: utf-8 -*-
# pylint: disable=unused-argument
"""Test base objects."""
from edfred.oob.messaging.sqs import SQSMessage, SQSQueue
from edfred.oob.utils import underscore_namedtuple
from boto3 import client
from moto import mock_sqs
import os


@mock_sqs
def test_sqs_message():
    """Test SQS Event."""
    sqs_event = {
        "messageId": "059f36b4-87a3-44ab-83d2-661975830a7d",
        "body": "Test message.",
        "attributes": {
            "ApproximateReceiveCount": "1",
            "SentTimestamp": "1545082649183",
            "SenderId": "AIDAIENQZJOLO23YVJ4VO",
            "ApproximateFirstReceiveTimestamp": "1545082649185",
        },
        "messageAttributes": {"oneWord": "CORONA", "oneInt": 19, "oneByte": b"bytes"},
        "eventSource": "aws:sqs",
        "eventSourceARN": "arn:aws:sqs:eu-west-1:123456789012:my-queue",
        "awsRegion": "eu-west-1",
    }

    message_attributes_schema = {
        "oneInt": {"DataType": "Number", "StringValue": "19"},
        "oneWord": {"DataType": "String", "StringValue": "CORONA"},
        "oneByte": {"DataType": "Binary", "BinaryValue": b"bytes"},
    }
    sqs = client("sqs", region_name="eu-west-1")
    sqs.create_queue(QueueName="my-queue")
    queue_url = sqs.get_queue_url(QueueName="my-queue", QueueOwnerAWSAccountId="123456789012").get("QueueUrl")

    message = SQSMessage(
        body=sqs_event.get("body", None),
        message_attributes=sqs_event.get("messageAttributes", {}),
        queue_url=queue_url,
        receipt_handle=None,
    )

    assert message.body == sqs_event.get("body", None)
    assert message.message_attributes == sqs_event.get("messageAttributes", {})
    assert message.message_attributes_schema == message_attributes_schema

    message.send()
    attributes = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["All"])["Attributes"]
    assert attributes["ApproximateNumberOfMessages"] == "1"
    assert message.id

    sqs_message_dict = sqs.receive_message(QueueUrl="my-queue")["Messages"][0]
    message.receipt_handle = sqs_message_dict["ReceiptHandle"]
    assert sqs_message_dict["MD5OfBody"] == message.body_md5

    message.delete()
    attributes = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["All"])["Attributes"]
    assert attributes["ApproximateNumberOfMessages"] == "0"


@mock_sqs
def test_sqs_queue():
    sqs = client("sqs", region_name="eu-west-1")
    sqs.create_queue(QueueName="my-queue")

    queue = SQSQueue(name="my-queue", account=os.getenv("AWS_ACCOUNT_ID"), region=os.getenv("AWS_DEFAULT_REGION"))

    assert queue.arn == "arn:aws:sqs:eu-west-1:123456789012:my-queue"

    message1 = queue.send_message("message1")
    message2 = queue.send_message("message2")
    message3 = queue.send_message("message3")

    assert queue.number_of_messages == 3

    message = queue.receive_message()
    assert queue.number_of_messages == 2

    queue.purge()
    assert queue.number_of_messages == 0

    message1 = queue.send_message("message1")
    message2 = queue.send_message("message2")
    message3 = queue.send_message("message3")

    messages = queue.receive_message_batch(5)
    assert len(messages) == 3

    queue.change_message_visibility_batch([m.receipt_handle for m in messages], 13)

    queue.delete_message_batch([m.receipt_handle for m in messages])

    assert queue.number_of_messages == 0
