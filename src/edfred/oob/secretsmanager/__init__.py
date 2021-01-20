import json
from typing import List, ClassVar, Tuple
from dataclasses import dataclass, InitVar, field, asdict
from boto3 import client, Session
from edfred.oob.utils import underscore_namedtuple


@dataclass
class SecretValue:
    secret_id: str
    secret_string: str = field(init=False, default="")
    attributes: Tuple = field(init=False, default=None)
    client: ClassVar[Session] = client("secretsmanager")

    def __post_init__(self):
        secret_string = self.client.get_secret_value(SecretId=self.secret_id)["SecretString"]
        jstring = json.loads(secret_string)
        attributes = {}
        if isinstance(jstring, dict):
            attributes = jstring
        if isinstance(jstring, list):
            for d in jstring:
                for k, v in d.items():
                    attributes[k] = v
        self.secret_string = secret_string
        self.attributes = underscore_namedtuple("SecretValueAttributes", attributes)
