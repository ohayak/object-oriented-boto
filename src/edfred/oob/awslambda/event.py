from collections import namedtuple
import inflection
import operator
from functools import reduce
from typing import ClassVar, List, Dict
from dataclasses import dataclass, field, asdict, InitVar
from . import Event, Handler
from edfred.oob.s3 import S3Object, S3Bucket
from edfred.oob.utils import underscore_namedtuple
from edfred.oob.messaging.sqs import SQSQueue, SQSMessage
from urllib.parse import unquote_plus


@dataclass
class SQSEvent(Event):
    """SQS Message Event class."""

    queue: SQSQueue = field(init=False)
    message: SQSMessage = field(init=False)

    def parse(self, payload, context):
        """Initialize the class."""
        sqs_event = payload["Records"][0]
        self.queue = SQSQueue(sqs_event.get("eventSourceARN", None))
        self.message = SQSMessage(
            body=sqs_event.get("body", None),
            body_md5=sqs_event.get("md5OfBody", None),
            region=sqs_event.get("awsRegion", None),
            attributes=underscore_namedtuple("Attributes", sqs_event.get("attributes", {})),
            message_attributes=sqs_event.get("messageAttributes", {}),
            queue_url=self.queue.url,
            id=sqs_event.get("messageId", None),
            receipt_handle=sqs_event.get("receiptHandle", None),
        )


@dataclass
class S3Event(Event):
    """SQS Message Event class."""

    bucket: S3Bucket = field(init=False)
    s3object: S3Object = field(init=False)

    def parse(self, payload, context):
        """Initialize the class."""
        s3_event = payload["Records"][0]
        self.bucket = S3Bucket(arn=s3_event["s3"]["bucket"]["arn"])
        self.s3object = S3Object(
            bucket_name=self.bucket.name, key=unquote_plus(payload["Records"][0]["s3"]["object"]["key"])
        )


@dataclass
class SFNEvent(Event):
    """Step Function Event class."""

    result_path: str = None
    sfninput: Dict = field(init=False)
    result: Dict = field(init=False)

    def parse(self, payload, context):
        """Initialize the class."""
        if self.result_path:
            result_path_keys = self.result_path.split(".")
            result = reduce(operator.getitem, result_path_keys, payload)
            sfninput = payload.pop(result_path_keys[0], {})
        else:
            sfninput = {}
            result = payload

        self.sfninput = sfninput
        self.result = result


@dataclass
class APIGatewayEvent(Event):
    """API Gateway Event class."""

    _body: str = field(init=False)
    _path_parameters: dict = field(init=False)
    _query_parameters: dict = field(init=False)
    _event_context: dict = field(init=False)
    _headers: dict = field(init=False)

    def parse(self, payload, context):
        self._body = payload.get("body", None)
        self._path_parameters = payload.get("pathParameters", {})
        self._query_parameters = payload.get("queryStringParameters", {})
        self._event_context = payload.get("eventContext", {})
        self._headers = payload.get("headers", {})

    @property
    def body(self):
        """Return string repr of body."""
        return str(self._body)

    @property
    def headers(self):
        """Return payload headers as namedtuple."""
        payload = {inflection.underscore(k): v for k, v, in self._headers.items()}
        HeadersTuple = namedtuple("HeadersTuple", sorted(payload))
        the_tuple = HeadersTuple(**payload)
        return the_tuple

    @property
    def path(self):
        """Return payload path parameters as namedtuple."""
        payload = {inflection.underscore(k): v for k, v, in self._path_parameters.items()}
        PathTuple = namedtuple("PathTuple", sorted(payload))
        the_tuple = PathTuple(**payload)
        return the_tuple

    @property
    def query(self):
        """Return payload query string as namedtuple."""
        payload = {inflection.underscore(k): v for k, v, in self._query_parameters.items()}
        QueryTuple = namedtuple("QueryTuple", sorted(payload))
        the_tuple = QueryTuple(**payload)
        return the_tuple
