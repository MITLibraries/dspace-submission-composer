from __future__ import annotations

import logging
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from typing import TYPE_CHECKING

from boto3 import client

if TYPE_CHECKING:
    from mypy_boto3_ses.type_defs import SendRawEmailResponseTypeDef

logger = logging.getLogger(__name__)


class SESClient:
    """A class to perform common SES operations for this application."""

    def __init__(self, region: str) -> None:
        self.client = client("ses", region_name=region)

    def create_and_send_email(
        self,
        subject: str,
        attachment_content: str,
        attachment_name: str,
        source_email_address: str,
        recipient_email_address: str,
    ) -> None:
        """Create an email message and send it via SES.

        Args:
           subject: The subject of the email.
           attachment_content: The content of the email attachment.
           attachment_name: The name of the email attachment.
           source_email_address: The email address of the sender.
           recipient_email_address: The email address of the receipient.
        """
        message = self._create_email(subject, attachment_content, attachment_name)
        self._send_email(source_email_address, recipient_email_address, message)
        logger.debug(f"Logs sent to {recipient_email_address}")

    def _create_email(
        self,
        subject: str,
        attachment_content: str,
        attachment_name: str,
    ) -> MIMEMultipart:
        """Create an email.

        Args:
            subject: The subject of the email.
            attachment_content: The content of the email attachment.
            attachment_name: The name of the email attachment.
        """
        message = MIMEMultipart()
        message["Subject"] = subject
        attachment_object = MIMEApplication(attachment_content)
        attachment_object.add_header(
            "Content-Disposition", "attachment", filename=attachment_name
        )
        message.attach(attachment_object)
        return message

    def _send_email(
        self,
        source_email_address: str,
        recipient_email_address: str,
        message: MIMEMultipart,
    ) -> SendRawEmailResponseTypeDef:
        """Send email via SES.

        Args:
            source_email_address: The email address of the sender.
            recipient_email_address: The email address of the receipient.
            message: The message to be sent.
        """
        return self.client.send_raw_email(
            Source=source_email_address,
            Destinations=[recipient_email_address],
            RawMessage={
                "Data": message.as_string(),
            },
        )
