from edfred.oob.messaging import MessageAttribute


def test_message_attribute():
    message_attribute = MessageAttribute(name="oneWord", value="CORONA")
    assert message_attribute.schema == {"DataType": "String", "StringValue": "CORONA"}
    message_attribute = MessageAttribute(name="oneInt", value=19)
    assert message_attribute.schema == {"DataType": "Number", "StringValue": "19"}
    message_attribute = MessageAttribute(name="oneByte", value=b"bytes")
    assert message_attribute.schema == {"DataType": "Binary", "BinaryValue": b"bytes"}
