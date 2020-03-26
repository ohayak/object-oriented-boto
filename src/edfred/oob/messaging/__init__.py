from dataclasses import dataclass, InitVar, field, asdict


@dataclass
class MessageAttribute:
    """[summary]
    
    Raises:
        TypeError: Only supports the following data types: str, int, float and None value
    """

    name: str
    value: object
    schema: dict = field(init=False, default_factory=dict)

    def __post_init__(self):
        val = self.value
        if val is None or type(val) == str:
            key = "StringValue"
            dtype = "String"
        elif type(val) in [int, float]:
            key = "StringValue"
            dtype = "Number"
        elif isinstance(val, bytes):
            key = "BinaryValue"
            dtype = "Binary"
        else:
            raise TypeError("Only supports the following data types: str, int, float and None value")

        if key == "StringValue":
            data = str(val)
        if key == "BinaryValue":
            data = bytes(val)

        self.schema[key] = data
        self.schema["DataType"] = dtype
