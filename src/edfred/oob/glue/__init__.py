import sys
import re
import os
from typing import ClassVar, List, Dict
from dataclasses import dataclass, field, asdict, InitVar
from boto3 import Session
from awsglue.utils import getResolvedOptions


@dataclass
class GlueJob:
    """Base Job."""

    arguments: Dict = None
    environ: Dict = None
    job_name: str = field(init=False)
    aws_region: str = field(init=False)
    aws_account_id: str = field(init=False)

    def __post_init__(self):
        """Initialize the job."""
        if not self.environ:
            self.environ = os.environ.copy()
        if not self.arguments:
            self.arguments = {}

        args = " ".join(sys.argv[1:])
        options_names = map(lambda x: x.group(1), re.finditer(r"--([\d\w-]*)", args))
        for option in options_names:
            try:
                self.arguments[option] = getResolvedOptions(sys.argv, [option])[option]
            except KeyError:
                continue
            except RuntimeError as e:
                if "Using reserved arguments" in str(e):
                    continue
        self.job_name = getResolvedOptions(sys.argv, ["JOB_NAME"]).get("JOB_NAME", "")
        self.aws_region = self.environ.get("AWS_REGION", "")
        if "AWS_ACCOUNT_ID" not in self.environ:
            sts = Session().client("sts")
            self.environ["AWS_ACCOUNT_ID"] = sts.get_caller_identity()["Account"]
        self.aws_account_id = self.environ.get("AWS_ACCOUNT_ID", "")

    def main(self, *args, **kwargs):
        """Stub main method."""
        raise NotImplementedError
