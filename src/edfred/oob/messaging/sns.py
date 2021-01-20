from dataclasses import dataclass, InitVar, field, asdict
from typing import ClassVar, List, Dict, Tuple
from boto3 import client, Session
from edfred.oob.utils import underscore_namedtuple
from . import MessageAttribute


@dataclass
class SNSBase:
    client: ClassVar[Session] = client("sns")


@dataclass
class SNSNotification(SNSBase):
    message: str = None
    subject: str = None
    structure: str = "text"
    timestamp: str = None
    signature: str = None
    signing_url: str = None
    message_id: str = None
    message_attributes: Dict = None
    message_attributes_schema: Dict = None
    type: str = None

    def __post_init__(self):
        if self.message_attributes:
            self.message_attributes_schema = {
                k: MessageAttribute(k, v).schema for k, v, in self.message_attributes.items()
            }
        elif self.message_attributes_schema:
            self.message_attributes = {}
            for k, v in self.message_attributes_schema.items():
                if "StringValue" in v:
                    self.message_attributes[k] = v["StringValue"]
                if "BinaryValue" in v:
                    self.message_attributes[k] = v["BinaryValue"]
        else:
            self.message_attributes = {}
            self.message_attributes_schema = {}


@dataclass
class SNSTopic(SNSBase):
    arn: str = None
    phone: str = None
    attributes: Dict = field(init=False)

    def __post_init__(self):
        self.attributes = underscore_namedtuple(
            "SNSTopic", self.client.get_topic_attributes(TopicArn=self.arn).get("Attributes", {})
        )

    def publish(self, message: str, subject: str = None, structure: str = "text", attributes: Dict = None) -> str:
        payload = dict(Message=message)
        if self.arn:
            payload["TopicArn"] = self.arn
        elif self.phone:
            payload["PhoneNumber"] = self.phone
        else:
            raise (ValueError("you must specify a value for the TopicArn or PhoneNumber parameters."))
        if subject:
            payload["Subject"] = subject
        if attributes:
            payload["MessageAttributes"] = attributes
        if structure == "json":
            payload["MessageStructure"] = "json"
        return self.client.publish(**payload)["MessageId"]


@dataclass
class SNSSubscription(SNSBase):
    arn: str = None
    attributes: Dict = field(init=False)

    def __post_init__(self):
        self.attributes = underscore_namedtuple(
            "SNSSubscriptionAttributes",
            self.client.get_subscription_attributes(SubscriptionArn=self.arn).get("Attributes", {}),
        )

    def unsubscribe(self):
        self.client.unsubscribe(SubscriptionArn=self.arn)


@dataclass
class SNSTopicNotification(SNSNotification):
    topic_arn: str = None

    def __post_init__(self):
        SNSNotification.__post_init__(self)
        if not self.topic_arn:
            raise ValueError("topic_arn must be defined and not None.")

    def publish(self) -> str:
        payload = dict(TopicArn=self.topic_arn, Message=self.message)
        if self.subject:
            payload["Subject"] = self.subject
        if self.message_attributes:
            payload["MessageAttributes"] = self.message_attributes_schema
        if self.structure == "json":
            payload["MessageStructure"] = "json"
        return self.client.publish(**payload)["MessageId"]
