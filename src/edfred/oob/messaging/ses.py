import os
from typing import ClassVar, List, Dict
from dataclasses import dataclass, field, asdict, InitVar
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from boto3 import client, Session
from botocore.exceptions import ClientError


@dataclass
class SESMessage:
    sender: str
    recipient: List[str]
    subject: str
    body_text: str
    body_html: str = ""
    attachments: List[str] = field(default_factory=list)
    charset: str = "UTF-8"
    client: ClassVar[Session] = client("ses")

    def __format_email(self) -> MIMEMultipart:
        msg = MIMEMultipart("mixed")
        # Add subject, from and to lines.
        msg["Subject"] = self.subject
        msg["From"] = self.sender
        msg["To"] = ", ".join(self.recipient)

        # Create a multipart/alternative child container.
        msg_body = MIMEMultipart("alternative")

        # Encode the text and HTML content and set the character encoding. This step is
        # necessary if you're sending a message with characters outside the ASCII range.
        if self.body_html == "":
            self.body_html = f"<!DOCTYPE html><html><head></head><body><p>{self.body_text}</p></body></html>"
        textpart = MIMEText(self.body_text.encode(self.charset), "plain", self.charset)
        htmlpart = MIMEText(self.body_html.encode(self.charset), "html", self.charset)

        # Add the text and HTML parts to the child container.
        msg_body.attach(textpart)
        msg_body.attach(htmlpart)

        # Attach the multipart/alternative child container to the multipart/mixed
        # parent container.
        msg.attach(msg_body)

        # Define the attachment part and encode it using MIMEApplication.
        for attachment in self.attachments:
            att = MIMEApplication(open(attachment, "rb").read())

            # Add a header to tell the email client to treat this part as an attachment,
            # and to give the attachment a name.
            att.add_header("Content-Disposition", "attachment", filename=os.path.basename(attachment))
            # Add the attachment to the parent container
            msg.attach(att)
        return msg

    def send(self) -> Dict:
        email = self.__format_email()
        response = self.client.send_raw_email(
            Source=email["From"], Destinations=self.recipient, RawMessage={"Data": email.as_string()}
        )
        return response
