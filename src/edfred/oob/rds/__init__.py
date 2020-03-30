import re
from dataclasses import dataclass, field


@dataclass
class AWSJdbc:
    url: str
    awsurl: str = field(init=False, default=None)
    engine: str = field(init=False, default=None)
    identifier: str = field(init=False, default=None)
    account_adress: str = field(init=False, default=None)
    region: str = field(init=False, default=None)
    aws_service: str = field(init=False, default=None)
    port: str = field(init=False, default=None)
    dbname: str = field(init=False, default=None)

    def __post_init__(self):
        regex = r"jdbc:(\w+)://(([\w\d-]+).([\w\d]+).([\w\d-]+).([\w\d-]+).amazonaws.com):(\d+)/([\w\d-]+)"
        match = re.match(regex, self.url)
        self.awsurl = match.group(0)
        self.engine = match.group(1)
        self.host = match.group(2)
        self.identifier = match.group(3)
        self.account_adress = match.group(4)
        self.region = match.group(5)
        self.aws_service = match.group(6)
        self.port = match.group(7)
        self.dbname = match.group(8)
