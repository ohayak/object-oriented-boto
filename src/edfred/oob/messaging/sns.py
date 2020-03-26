from dataclasses import dataclass, InitVar, field, asdict
from typing import ClassVar, List, Dict, Tuple
from boto3 import client, Session
from edfred.oob.utils import underscore_namedtuple
from . import MessageAttribute


@dataclass
class SNSMessage:
    timestamp: str = None
    signature: str = None
    signing_url: str = None
    id: str = None
    body: str = None
    structure: str = "text"
    attributes: Dict = {}
    attributes_schema: Dict = field(init=False)
    notification_type: str = None
    subscription: Tuple = None
    topic: Tuple = None

    def __post_init__(self):
        self.attributes_schema = {k: MessageAttribute(k, v) for k, v, in self.attributes.items()}


@dataclass
class SNSTopic:
    arn: str = None
    phone: str = None
    subject: str = None
    client: ClassVar[Session] = client("sns")

    def publish(self, message: SNSMessage):
        payload = dict(Message=message.body, Subject=self.subject, MessageAttributes=message.attributes_schema)
        if self.arn:
            payload["TopicArn"] = self.arn
        elif self.phone:
            payload["PhoneNumber"] = self.phone
        else:
            raise (ValueError("you must specify a value for the TopicArn or PhoneNumber parameters."))
        if message.structure == "json":
            payload["MessageStructure"] = "json"
        message.topic = underscore_namedtuple("Topic", asdict(self))
        return self.client.publish(payload)


@dataclass
class SNSSubscription:
    arn: str
    unsubscription_url: str
