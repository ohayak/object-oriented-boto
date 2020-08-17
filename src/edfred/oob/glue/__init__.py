import sys
import re
import os
from logging import Logger
from typing import ClassVar, List, Dict
from dataclasses import dataclass, field, asdict, InitVar
from boto3 import Session
from awsglue.job import Job
from awsglue.utils import getResolvedOptions, GlueArgumentError

from awsglue.context import GlueContext
from pyspark import SparkContext
from pyspark.sql import SparkSession



@dataclass
class GlueJob:
    """Base Job."""

    arguments: Dict = None
    environ: Dict = None
    job_name: str = field(init=False)
    aws_region: str = field(init=False)
    aws_account_id: str = field(init=False)
    spark_context: SparkContext = field(init=False)
    glue_context: GlueContext = field(init=False)
    spark_session: SparkSession = field(init=False)
    glue_job: Job = field(init=False)
    logger: Logger = field(init=False)

    def __post_init__(self):
        """Initialize the job."""
        if not self.environ:
            self.environ = os.environ.copy()
        if not self.arguments:
            self.arguments = {}

        args = " ".join(sys.argv[1:])
        options_names = map(lambda x: x.group(1), re.finditer(r"--([\d\w-]*)", args))
        reserved_names = Job.continuation_options() + Job.job_bookmark_options() + Job.job_bookmark_range_options() + Job.id_params() + Job.encryption_type_options()
        for option in set(options_names) - set(reserved_names):
            try:
                self.arguments[option] = getResolvedOptions(sys.argv, [option])[option]
            except Exception as e:
                print('Skip argument parsing Error:')
                print(e)
                continue
        self.job_name = getResolvedOptions(sys.argv, ["JOB_NAME"]).get("JOB_NAME", "")
        self.aws_region = self.environ.get("AWS_REGION", "")
        if "AWS_ACCOUNT_ID" not in self.environ:
            sts = Session().client("sts")
            self.environ["AWS_ACCOUNT_ID"] = sts.get_caller_identity()["Account"]
        self.aws_account_id = self.environ.get("AWS_ACCOUNT_ID", "")

        self.spark_context = SparkContext()
        self.glue_context = GlueContext(self.spark_context)
        self.spark_session = self.glue_context.spark_session
        self.glue_job = Job(self.glue_context)
        self.logger = self.glue_context.get_logger()

    def main(self, *args, **kwargs):
        """Stub main method."""
        raise NotImplementedError
