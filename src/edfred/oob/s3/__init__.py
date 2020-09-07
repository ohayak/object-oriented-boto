from typing import List, ClassVar, Tuple, Union, IO, Dict, Optional
from dataclasses import dataclass, InitVar, field, asdict
from io import BytesIO
from boto3 import client, Session
from edfred.oob.utils import underscore_namedtuple


@dataclass
class S3Base:
    client: ClassVar[Session] = client("s3")


@dataclass
class S3Bucket(S3Base):
    arn: str = None
    name: str = None
    region: str = None
    __list_keys_args: Dict = field(init=False, default_factory=dict, repr=False)

    def __post_init__(self):
        arn = self.arn
        name = self.name

        if arn and name:
            raise AttributeError("BucketManager can be initializied using either arn or bucket name not both.")
        if arn:
            self.name = arn.split(":")[-1]
        else:
            self.arn = f"arn:aws:s3:::{name}"

        self.region = self.client.get_bucket_location(Bucket=self.name)["LocationConstraint"]

    def head(self):
        self.client.head_bucket(Bucket=self.name)

    def upload_file(
        self, file: Union[str, IO[bytes]], dest: str, consistent_write=True, wait_delay: int = 5
    ) -> Optional["S3Object"]:
        """Upload file/fileobj to destination, blocking option waits for the uploaded file to be available"""

        if isinstance(file, str):
            self.client.upload_file(file, self.name, dest)
        else:
            self.client.upload_fileobj(file, self.name, dest)

        if consistent_write:
            waiter = self.client.get_waiter("object_exists")
            waiter.wait(Bucket=self.name, Key=dest, WaiterConfig={"Delay": wait_delay})
            return S3Object(bucket_name=self.name, key=dest)

    def get_object(self, object_key):
        return S3Object(bucket_name=self.name, key=object_key)

    def _list_keys(self, list_keys_args, max_keys):
        response = self.client.list_objects_v2(**list_keys_args)
        keys = list(o["Key"] for o in response.get("Contents", []))
        if "NextContinuationToken" in response:
            list_keys_args.update({"ContinuationToken": response["NextContinuationToken"]})
        else:
            list_keys_args.pop("ContinuationToken", None)
        if not max_keys:
            while "NextContinuationToken" in response:
                list_keys_args.update({"ContinuationToken": response["NextContinuationToken"]})
                response = self.client.list_objects_v2(**dict(list_keys_args))
                keys += list(o["Key"] for o in response.get("Contents", []))
        else:
            while "NextContinuationToken" in response and len(keys) < max_keys:
                list_keys_args.update({"ContinuationToken": response["NextContinuationToken"]})
                response = self.client.list_objects_v2(**dict(list_keys_args))
                keys += list(o["Key"] for o in response.get("Contents", []))
        return keys

    def list_keys(self, prefix="", max_keys: int = None) -> List[str]:
        list_keys_args = {"Bucket": self.name, "Prefix": prefix, "MaxKeys": min(1000, max_keys if max_keys else 1000)}
        return self._list_keys(list_keys_args, max_keys)

    def list_keys_paginator(self, prefix="", max_keys: int = 1000):
        return iter(S3BucketPaginator(self, prefix, max_keys))

    def delete_object(self, key):
        return self.client.delete_object(Bucket=self.name, Key=key)

    def delete_objects(self, keys: List[str] = None, prefix: str = None) -> List[Dict]:

        if prefix:
            keys = self.list_keys(prefix)

        responses = []
        for batch_no in range(0, len(keys), 1000):
            responses.append(
                self.client.delete_objects(
                    Bucket=self.name, Delete={"Objects": [{"Key": k} for k in keys[batch_no : batch_no + 1000]]}
                )
            )
        return responses


class S3BucketPaginator:
    def __init__(self, bucket: "S3Bucket", prefix: str, max_keys: int):
        self.bucket = bucket
        self.prefix = prefix
        self.max_keys = max_keys
        self.list_keys_args = None
        self.done = None

    def __iter__(self):
        self.done = False
        self.list_keys_args = {"Bucket": self.bucket.name, "Prefix": self.prefix, "MaxKeys": min(1000, self.max_keys)}
        return self

    def __next__(self):
        if self.done:
            raise StopIteration
        keys = self.bucket._list_keys(self.list_keys_args, self.max_keys)
        if "ContinuationToken" not in self.list_keys_args:
            self.done = True
        return keys


@dataclass
class S3Object(S3Base):
    bucket_name: str = None
    key: str = None
    region: str = field(default=None, init=False)
    filename: str = field(default=None, init=False)
    prefix: str = field(default=None, init=False)
    suffix: str = field(default=None, init=False)
    is_folder: bool = field(default=None, init=False)

    def __post_init__(self):
        key_split = self.key.split("/")
        self.is_folder = self.key.endswith("/")
        self.filename = key_split[-1]
        self.prefix = key_split[:-1]
        self.suffix = key_split[-1].split(".")[-1]
        self.attributes

    @property
    def attributes(self) -> Tuple:
        return underscore_namedtuple("ObjectHead", self.client.head_object(Bucket=self.bucket_name, Key=self.key))

    def download_fileobj(self) -> BytesIO:
        fileobj = BytesIO()
        self.client.download_fileobj(self.bucket_name, self.key, fileobj)
        fileobj.seek(0)
        return fileobj

    def copy_to(self, bucket, key) -> "S3Object":
        self.client.copy(Bucket=bucket, Key=key, CopySource={"Bucket": self.bucket_name, "Key": self.key})
        new = S3Object(bucket, key)
        return new

    def move_to(self, bucket, key) -> "S3Object":
        new = self.copy_to(bucket, key)
        self.delete()
        return new

    def delete(self):
        return self.client.delete_object(Bucket=self.bucket_name, Key=self.key)

    def download_to(self, dest):
        return self.client.download_file(self.bucket_name, self.key, dest)

    def restore_object(self):
        return self.client.restore_object(self.bucket_name, self.key)
