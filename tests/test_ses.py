import logging
from email.mime.multipart import MIMEMultipart
from http import HTTPStatus
from io import StringIO


def test_ses_create_and_send_email(caplog, mocked_ses, ses_client):
    with caplog.at_level(logging.DEBUG):
        ses_client.create_and_send_email(
            subject="Email subject",
            source_email_address="noreply@example.com",
            recipient_email_addresses=["test@example.com"],
            message_body="Message body in plain-text",
        )
        assert "Logs sent to ['test@example.com']" in caplog.text


def test_ses_create_email(ses_client):
    message = ses_client._create_email(  # noqa: SLF001
        subject="Email subject",
        message_body="Message body in plain-text",
        attachments=[("errors.csv", StringIO())],
    )
    assert message["Subject"] == "Email subject"
    assert "Message body in plain-text" in message.get_payload()[0].as_string()
    assert message.get_payload()[1].get_filename() == "errors.csv"


def test_ses_send_email(mocked_ses, ses_client):
    message = MIMEMultipart()
    response = ses_client._send_email(  # noqa: SLF001
        source_email_address="noreply@example.com",
        recipient_email_addresses=["test@example.com"],
        message=message,
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == HTTPStatus.OK
