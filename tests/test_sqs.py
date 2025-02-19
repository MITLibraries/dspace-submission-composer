from http import HTTPStatus

import pytest
from botocore.exceptions import ClientError

from dsc.exceptions import InvalidSQSMessageError


def test_sqs_create_dss_message_attributes(sqs_client, submission_message_attributes):
    dss_message_attributes = sqs_client.create_dss_message_attributes(
        item_identifier="123",
        submission_source="Submission system",
        output_queue="DSS queue",
    )
    assert dss_message_attributes == submission_message_attributes


def test_sqs_create_dss_message_body(sqs_client, submission_message_body):
    dss_message_body = sqs_client.create_dss_message_body(
        submission_system="DSpace@MIT",
        collection_handle="123.4/5678",
        metadata_s3_uri="s3://dsc/10.1002-term.3131.json",
        bitstream_s3_uris=["s3://dsc/10.1002-term.3131.pdf"],
    )
    assert dss_message_body == submission_message_body


def test_sqs_delete_success(
    mocked_sqs_output,
    sqs_client,
    result_message_attributes,
    result_message_body,
):
    sqs_client.send(
        message_attributes=result_message_attributes,
        message_body=result_message_body,
    )
    message = next(sqs_client.receive())
    response = sqs_client.delete(
        receipt_handle=message["ReceiptHandle"], message_id=message["MessageId"]
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == HTTPStatus.OK


def test_sqs_delete_invalid_receipt_handle_raises_error(mocked_sqs_output, sqs_client):
    with pytest.raises(ClientError) as exception_info:
        sqs_client.delete(
            receipt_handle="abc",
            message_id="def",
        )

    assert str(exception_info.value) == (
        "An error occurred (ReceiptHandleIsInvalid) when calling "
        "the DeleteMessage operation: The input receipt handle is invalid."
    )


def test_sqs_send_success(
    mocked_sqs_input, sqs_client, submission_message_attributes, submission_message_body
):
    sqs_client.queue_name = "mock-input-queue"
    response = sqs_client.send(
        message_attributes=submission_message_attributes,
        message_body=submission_message_body,
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == HTTPStatus.OK


def test_sqs_send_nonexistent_queue_raises_error(
    mocked_sqs_input, sqs_client, submission_message_attributes, submission_message_body
):
    sqs_client.queue_name = "nonexistent"
    with pytest.raises(ClientError) as exception_info:
        sqs_client.send(
            message_attributes=submission_message_attributes,
            message_body=submission_message_body,
        )

    assert str(exception_info.value) == (
        "An error occurred (AWS.SimpleQueueService.NonExistentQueue) when "
        "calling the GetQueueUrl operation: The specified queue does not exist."
    )


def test_sqs_receive_success(
    mocked_sqs_output,
    sqs_client,
    result_message_attributes,
    result_message_body,
):
    sqs_client.send(
        message_attributes=result_message_attributes,
        message_body=result_message_body,
    )
    messages = sqs_client.receive()
    for message in messages:
        assert message["Body"] == str(result_message_body)
        assert message["MessageAttributes"] == result_message_attributes


def test_sqs_receive_nonexistent_queue_raises_error(mocked_sqs_output, sqs_client):
    sqs_client.queue_name = "nonexistent"

    with pytest.raises(ClientError) as exception_info:
        next(sqs_client.receive())

    assert str(exception_info.value) == (
        "An error occurred (AWS.SimpleQueueService.NonExistentQueue) when calling "
        "the GetQueueUrl operation: The specified queue does not exist."
    )


def test_sqs_parse_dss_result_message_success(
    mocked_sqs_output, sqs_client, result_message_attributes, result_message_body
):
    sqs_client.send(
        message_attributes=result_message_attributes,
        message_body=result_message_body,
    )
    messages = sqs_client.receive()
    identifier, message_body = sqs_client.parse_dss_result_message(
        sqs_message=next(messages)
    )
    assert identifier == "10.1002/term.3131"
    assert message_body["ItemHandle"] == "1721.1/131022"


def test_sqs_parse_dss_result_message_invalid_message_raises_exception(
    mocked_sqs_output,
    sqs_client,
):
    sqs_client.send(message_attributes={}, message_body="")
    messages = sqs_client.receive()
    with pytest.raises(InvalidSQSMessageError):
        sqs_client.parse_dss_result_message(
            sqs_message=next(messages),
        )


def test_sqs_validate_dss_result_message_success(sqs_client, result_message_valid):
    try:
        sqs_client.validate_dss_result_message(sqs_message=result_message_valid)
    except Exception:  # noqa: BLE001
        pytest.fail("An exception was raised.")


def test_sqs_validate_dss_result_message_no_receipt_handle_raises_error(
    mocked_sqs_input, result_message_valid, sqs_client
):
    # message without 'ReceiptHandle' is invalid
    result_message_invalid = dict(result_message_valid)
    result_message_invalid["ReceiptHandle"] = None

    with pytest.raises(
        InvalidSQSMessageError, match="Failed to retrieve 'ReceiptHandle' from message"
    ):
        sqs_client.validate_dss_result_message(sqs_message=result_message_invalid)


def test_sqs_validate_message_attributes_invalid_raises_error(
    mocked_sqs_input, result_message_valid, sqs_client
):
    # message without 'MessageAttributes' is invalid
    result_message_invalid = dict(result_message_valid)
    result_message_invalid["MessageAttributes"] = {}

    with pytest.raises(
        InvalidSQSMessageError, match="Failed to parse message attributes"
    ):
        sqs_client.validate_message_attributes(sqs_message=result_message_invalid)


def test_sqs_validate_message_body_invalid_raises_error(
    mocked_sqs_input, result_message_valid, sqs_client
):
    result_message_invalid = dict(result_message_valid)
    result_message_invalid["Body"] = "{}"

    with pytest.raises(InvalidSQSMessageError, match="Failed to parse message body"):
        sqs_client.validate_message_body(sqs_message=result_message_invalid)
