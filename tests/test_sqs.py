from http import HTTPStatus

import pytest
from botocore.exceptions import ClientError

from dsc.exceptions import InvalidSQSMessageError


def test_sqs_create_dss_message_attributes(sqs_client, submission_message_attributes):
    dss_message_attributes = sqs_client.create_dss_message_attributes(
        package_id="123", submission_source="Submission system", output_queue="DSS queue"
    )
    assert dss_message_attributes == submission_message_attributes


def test_sqs_create_dss_message_body(sqs_client, submission_message_body):
    dss_message_body = sqs_client.create_dss_message_body(
        submission_system="DSpace@MIT",
        collection_handle="123.4/5678",
        metadata_s3_uri="s3://awd/10.1002-term.3131.json",
        bitstream_file_name="10.1002-term.3131.pdf",
        bitstream_s3_uri="s3://awd/10.1002-term.3131.pdf",
    )
    assert dss_message_body == submission_message_body


def test_sqs_delete_nonexistent_message_raises_error(mocked_sqs_output, sqs_client):
    with pytest.raises(ClientError):
        sqs_client.delete(receipt_handle="12345678")


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
    messages = sqs_client.receive()
    receipt_handle = next(messages)["ReceiptHandle"]
    response = sqs_client.delete(receipt_handle=receipt_handle)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == HTTPStatus.OK


def test_sqs_process_result_message(
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
    identifier, message_body = sqs_client.process_result_message(
        sqs_message=next(messages)
    )
    assert identifier == "10.1002/term.3131"
    assert message_body == {
        "Bitstreams": [
            {
                "BitstreamChecksum": {
                    "checkSumAlgorithm": "MD5",
                    "value": "a4e0f4930dfaff904fa3c6c85b0b8ecc",
                },
                "BitstreamName": "10.1002-term.3131.pdf",
                "BitstreamUUID": "a1b2c3d4e5",
            }
        ],
        "ItemHandle": "1721.1/131022",
        "ResultType": "success",
        "lastModified": "Thu Sep 09 17:56:39 UTC 2021",
    }


def test_sqs_process_result_message_raises_invalid_sqs_exception(
    mocked_sqs_output,
    sqs_client,
):
    sqs_client.send(message_attributes={}, message_body={})
    messages = sqs_client.receive()
    with pytest.raises(InvalidSQSMessageError):
        sqs_client.process_result_message(
            sqs_message=next(messages),
        )


def test_sqs_receive_raises_error_for_incorrect_queue(mocked_sqs_output, sqs_client):
    sqs_client.queue_name = "non-existent"
    with pytest.raises(ClientError):
        next(sqs_client.receive())


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


def test_sqs_send_raises_error_for_incorrect_queue(
    mocked_sqs_input, sqs_client, submission_message_attributes, submission_message_body
):
    sqs_client.queue_name = "non-existent"
    with pytest.raises(ClientError):
        sqs_client.send(
            message_attributes=submission_message_attributes,
            message_body=submission_message_body,
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


def test_sqs_validate_message_no_receipthandle_false(
    mocked_sqs_input, sqs_client, result_message_valid
):
    assert not sqs_client.validate_message(sqs_message={})


def test_sqs_validate_message_true(mocked_sqs_input, sqs_client, result_message_valid):
    assert sqs_client.validate_message(sqs_message=result_message_valid)


def test_sqs_validate_message_attributes_false(mocked_sqs_input, sqs_client):
    assert not sqs_client.validate_message_attributes(sqs_message={})


def test_sqs_validate_message_attributes_true(
    mocked_sqs_input, sqs_client, result_message_valid
):
    assert sqs_client.validate_message_attributes(sqs_message=result_message_valid)


def test_sqs_validate_message_body_false(caplog, mocked_sqs_input, sqs_client):
    assert not sqs_client.validate_message_body(sqs_message={None})


def test_sqs_validate_message_body_true(
    mocked_sqs_input, sqs_client, result_message_valid
):
    assert sqs_client.validate_message_body(sqs_message=result_message_valid)
