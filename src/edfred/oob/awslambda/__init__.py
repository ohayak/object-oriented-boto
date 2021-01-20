import os, logging
from typing import ClassVar, List, Dict
from dataclasses import dataclass, field, asdict, InitVar
from boto3.session import Session


@dataclass
class Event:
    """Base Event."""

    payload: Dict = field(init=False, default=None)
    context: Dict = field(init=False, default=None)

    def __call__(self, payload, context, **kwargs):
        """Wrap parse(), invoked by Handler."""
        self.payload = payload
        self.context = context
        self.parse(payload, context)
        return self

    def parse(self, payload, context):
        """Stub parse method."""
        pass


@dataclass
class Handler:
    """Base Handler."""

    event_parser: Event = field(default_factory=Event)
    environ: Dict = None
    aws_lambda_name: str = field(init=False)
    aws_region: str = field(init=False)
    aws_account_id: str = field(init=False)
    region: str = field(init=False)
    account_id: str = field(init=False)

    def __post_init__(self):
        """Initialize the handler."""
        if not self.environ:
            self.environ = os.environ.copy()
        self.aws_lambda_name = self.environ.get("AWS_LAMBDA_FUNCTION_NAME", "")
        self.aws_region = self.environ.get("AWS_REGION", "")
        if "AWS_ACCOUNT_ID" not in self.environ:
            sts = Session().client("sts")
            self.environ["AWS_ACCOUNT_ID"] = sts.get_caller_identity()["Account"]
        self.aws_account_id = self.environ.get("AWS_ACCOUNT_ID", "")
        self.region = self.environ.get("REGION", self.aws_region)
        self.account_id = self.environ.get("ACCOUNT_ID", self.aws_account_id)

        boto_level = os.getenv("BOTO_LOG_LEVEL", "WARNING")
        logging.getLogger("boto").setLevel(boto_level)
        logging.getLogger("boto3").setLevel(boto_level)
        logging.getLogger("botocore").setLevel(boto_level)
        logging.getLogger("urllib3").setLevel(boto_level)
        logging.getLogger("s3transfer").setLevel(boto_level)

    def __call__(self, payload, context, **kwargs):
        """Wrap perform(), invoked by AWS Lambda."""
        if "ManualCall" in payload:
            event = self.event_parser(
                payload["ManualCall"].get("Payload", {}), payload["ManualCall"].get("Context", context)
            )
            attributes = payload["ManualCall"].get("Attributes", {})
            environ = payload["ManualCall"].get("Environ", {})
            if environ:
                self.overwrite_environ(environ)
                self.__post_init__()
            return self.manual_call(event, attributes)
        else:
            environ = payload.get("Environ", {})
            if environ:
                self.overwrite_environ(environ)
                self.__post_init__()
            event = self.event_parser(payload, context)
            return self.perform(event)

    def overwrite_environ(self, environ: Dict):
        for k, v in environ.items():
            self.environ[k] = str(v)

    def manual_call(self, event: Event, attributes: Dict):
        return self.perform(event)

    def perform(self, event: Event):
        """Stub perform method."""
        raise NotImplementedError
