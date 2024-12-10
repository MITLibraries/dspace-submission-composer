import logging
from email.mime.multipart import MIMEMultipart
from http import HTTPStatus


def test_ses_create_and_send_email(caplog, mocked_ses, ses_client):
    with caplog.at_level(logging.DEBUG):
        ses_client.create_and_send_email(
            subject="Email subject",
            attachment_content="<html/>",
            attachment_name="attachment",
            source_email_address="noreply@example.com",
            recipient_email_address="test@example.com",
        )
        assert "Logs sent to test@example.com" in caplog.text


def test_ses_create_email(ses_client):
    message = ses_client._create_email(  # noqa: SLF001
        subject="Email subject",
        attachment_content="<html/>",
        attachment_name="attachment",
    )
    assert message["Subject"] == "Email subject"
    assert message.get_payload()[0].get_filename() == "attachment"


def test_ses_send_email(mocked_ses, ses_client):
    message = MIMEMultipart()
    response = ses_client._send_email(  # noqa: SLF001
        source_email_address="noreply@example.com",
        recipient_email_address="test@example.com",
        message=message,
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == HTTPStatus.OK
