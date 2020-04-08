import collections
import os
from typing import ClassVar, List, Dict
from dataclasses import dataclass, field, asdict, InitVar


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
    region: str = field(init=False)

    def __post_init__(self):
        """Initialize the handler."""
        if not self.environ:
            self.environ = os.environ.copy()
        self.aws_lambda_name = self.environ.get("AWS_LAMBDA_FUNCTION_NAME", "Lambda")
        self.aws_region = self.environ.get("AWS_REGION", "")
        self.region = self.environ.get("REGION", self.aws_region)

    def __call__(self, payload, context, **kwargs):
        """Wrap perform(), invoked by AWS Lambda."""
        if "ManualCall" in payload:
            event = self.event_parser(
                payload["ManualCall"].get("Payload", {}), payload["ManualCall"].get("Context", context)
            )
            attributes = payload["ManualCall"].get("Attributes", {})
            environ = payload["ManualCall"].get("Environ", {})
            self.overwrite_environ(environ)
            return self.manual_call(event, attributes)
        else:
            self.overwrite_environ(payload.get("Environ", {}))
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
