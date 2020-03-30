from dataclasses import dataclass, InitVar, field, asdict
from typing import ClassVar, List, Dict, Tuple, Generator
from boto3 import client, Session
from edfred.oob.utils import underscore_namedtuple
from . import MessageAttribute


@dataclass
class SQSBase:
    client: ClassVar[Session] = client("sqs")


@dataclass
class SQSMessage(SQSBase):
    queue_url: str
    body: str
    receipt_handle: str = None
    body_md5: str = None
    region: str = None
    attributes: Tuple = None
    message_attributes: Dict = None
    message_attributes_schema: Dict = None
    id: str = None
    groupe_id: str = None
    sequence_number: str = None

    def __post_init__(self):
        if self.message_attributes and not self.message_attributes_schema:
            self.message_attributes_schema = {
                k: MessageAttribute(k, v).schema for k, v, in self.message_attributes.items()
            }
        elif self.message_attributes_schema and not self.message_attributes:
            self.message_attributes = {}
            for k, v in self.message_attributes.items():
                if "StringValue" in v:
                    self.message_attributes[k] = v["StringValue"]
                if "BinaryValue" in v:
                    self.message_attributes[k] = v["BinaryValue"]
        else:
            self.message_attributes = {}
            self.message_attributes_schema = {}

    @staticmethod
    def duplicate(queue_url: str, message: "SQSMessage"):
        return SQSMessage(
            queue_url=queue_url, body=message.body, region=message.region, message_attributes=message.message_attributes
        )

    def change_visibility(self, visibility_timeout: int) -> Dict:
        return self.client.change_message_visibility(
            QueueUrl=self.queue_url, ReceiptHandle=self.receipt_handle, VisibilityTimeout=visibility_timeout
        )

    def delete(self) -> Dict:
        return self.client.delete_message(QueueUrl=self.queue_url, ReceiptHandle=self.receipt_handle)

    def send(self, delay: int = None) -> Dict:
        payload = dict(QueueUrl=self.queue_url, MessageBody=self.body, MessageAttributes=self.message_attributes_schema)
        if self.groupe_id:
            payload["GroupeId"] = self.groupe_id
        if delay:
            payload["DelaySeconds"] = delay
        response = self.client.send_message(**payload)
        self.id = response["MessageId"]
        self.body_md5 = response["MD5OfMessageBody"]
        self.sequence_number = response.get("SequenceNumber", None)
        return response


@dataclass
class SQSQueue(SQSBase):
    arn: str = None
    url: str = field(default=None, init=False)
    region: str = None
    account: str = None
    name: str = None

    def __post_init__(self):
        arn = self.arn
        region = self.region
        account = self.account
        name = self.name

        if arn and (region or account or name):
            raise AttributeError("Queue can be initializied using either arn or (region, account, name) not both.")
        elif arn:
            self.region, self.account, self.name = arn.split(":")[-3:]
        else:
            self.arn = f"arn:aws:sqs:{region}:{account}:{name}"

        self.url = self.client.get_queue_url(QueueName=self.name, QueueOwnerAWSAccountId=self.account).get("QueueUrl")

    @property
    def attributes(self) -> Dict:
        response = self.client.get_queue_attributes(QueueUrl=self.url, AttributeNames=["All"])
        return underscore_namedtuple("QueueAttributes", response["Attributes"])

    @property
    def number_of_messages(self) -> int:
        return int(self.attributes.approximate_number_of_messages)

    def __receive_message(self, batch_size, visibility_timeout=None, wait_time=0) -> Generator[SQSMessage, None, None]:
        request_arguments = {
            "QueueUrl": self.url,
            "AttributeNames": ["All"],
            "MessageAttributeNames": ["All"],
            "MaxNumberOfMessages": batch_size,
            "WaitTimeSeconds": wait_time,
        }
        if visibility_timeout:
            request_arguments["VisibilityTimeout"] = visibility_timeout
        response = self.client.receive_message(**request_arguments)
        for message in response["Messages"]:
            yield SQSMessage(
                body=message.get("Body", None),
                body_md5=message.get("MD5OfBody", None),
                region=message.get("Region", None),
                attributes=underscore_namedtuple("Attributes", message.get("Attributes", {})),
                message_attributes=message.get("MessageAttributes", {}),
                queue_url=self.url,
                id=message.get("MessageId", None),
                receipt_handle=message.get("ReceiptHandle", None),
            )

    def receive_message_batch(self, batch_size, visibility_timeout=None, wait_time=0) -> List[SQSMessage]:
        number_of_messages = int(self.number_of_messages)
        messages = []
        while len(messages) < min(batch_size, number_of_messages):
            missing = min(batch_size - len(messages), 10)
            messages.extend(list(self.__receive_message(missing, visibility_timeout, wait_time)))
        return messages

    def receive_message(self, visibility_timeout=None, wait_time=0) -> SQSMessage:
        return list(self.__receive_message(1, visibility_timeout, wait_time))[0]

    def send_message(self, body, message_attributes={}, delay: int = None) -> SQSMessage:
        message = SQSMessage(queue_url=self.url, body=body, message_attributes=message_attributes)
        message.send(delay)
        return message

    def delete_message(self, receipt_handle: str) -> Dict:
        return self.client.delete_message(QueueUrl=self.url, ReceiptHandle=receipt_handle)

    def delete_message_batch(self, receipt_handle_list: List[str]) -> Dict:
        return self.client.delete_message_batch(
            QueueUrl=self.url, Entries=[dict(Id=str(i), ReceiptHandle=m) for i, m in enumerate(receipt_handle_list)]
        )

    def purge(self) -> Dict:
        return self.client.purge_queue(QueueUrl=self.url)

    def change_message_visibility(self, receipt_handle: str, visibility_timeout: int) -> Dict:
        return self.client.change_message_visibility(
            QueueUrl=self.url, ReceiptHandle=receipt_handle, VisibilityTimeout=visibility_timeout
        )

    def change_message_visibility_batch(self, receipt_handle_list: List[str], visibility_timeout: int) -> Dict:
        return self.client.change_message_visibility_batch(
            QueueUrl=self.url,
            Entries=[
                dict(Id=str(i), ReceiptHandle=m, VisibilityTimeout=visibility_timeout)
                for i, m in enumerate(receipt_handle_list)
            ],
        )
