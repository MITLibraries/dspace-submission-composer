from __future__ import annotations

import logging
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import TYPE_CHECKING

from boto3 import client

if TYPE_CHECKING:  # pragma: no cover
    from io import StringIO

    from mypy_boto3_ses.type_defs import SendRawEmailResponseTypeDef

logger = logging.getLogger(__name__)


class SESClient:
    """A class to perform common SES operations for this application."""

    def __init__(self, region: str) -> None:
        self.client = client("ses", region_name=region)

    def create_and_send_email(
        self,
        subject: str,
        source_email_address: str,
        recipient_email_addresses: list[str],
        message_body: str,
        attachments: list[tuple] | None = None,
    ) -> None:
        """Create an email message and send it via SES.

        Args:
            subject: The subject of the email.
            source_email_address: The email address of the sender.
            recipient_email_addresses: The email address of the receipient.
            message_body: Message body rendered in plain-text.
            attachments: Attachments to include in an email, represented as
                a list of tuples containing: filename, content type, content.
        """
        message = self._create_email(subject, message_body, attachments)
        self._send_email(source_email_address, recipient_email_addresses, message)
        logger.debug(f"Logs sent to {recipient_email_addresses}")

    def _create_email(
        self,
        subject: str,
        message_body: str,
        attachments: list | None = None,
    ) -> MIMEMultipart:
        message = MIMEMultipart()
        message["Subject"] = subject
        message.attach(MIMEText(message_body, "plain"))
        if attachments:
            for filename, content in attachments:
                attachment = self._create_attachment(filename, content)
                message.attach(attachment)

        return message

    def _create_attachment(self, filename: str, content: StringIO) -> MIMEApplication:
        content.seek(0)
        attachment = MIMEApplication(content.read())
        attachment.add_header("Content-Disposition", "attachment", filename=filename)
        return attachment

    def _send_email(
        self,
        source_email_address: str,
        recipient_email_addresses: list[str],
        message: MIMEMultipart,
    ) -> SendRawEmailResponseTypeDef:
        """Send email via SES.

        Args:
            source_email_address: The email address of the sender.
            recipient_email_addresses: The email address of the receipient.
            message: The message to be sent.
        """
        return self.client.send_raw_email(
            Source=source_email_address,
            Destinations=recipient_email_addresses,
            RawMessage={
                "Data": message.as_string(),
            },
        )
