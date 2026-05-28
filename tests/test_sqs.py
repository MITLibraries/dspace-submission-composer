import json
from http import HTTPStatus

import pytest
from botocore.exceptions import ClientError


@pytest.fixture
def submission_message_attributes():
    return {
        "PackageID": {"DataType": "String", "StringValue": "123"},
        "SubmissionSource": {"DataType": "String", "StringValue": "Submission system"},
        "OutputQueue": {"DataType": "String", "StringValue": "DSS queue"},
    }


@pytest.fixture
def submission_message_body_for_create_operation():
    return json.dumps(
        {
            "Operation": "create",
            "SubmissionSystem": "DSpace@MIT",
            "CollectionHandle": "123.4/5678",
            "ItemHandle": None,
            "MetadataLocation": "s3://dsc/10.1002-term.3131.json",
            "Files": [
                {
                    "BitstreamName": "10.1002-term.3131.pdf",
                    "FileLocation": "s3://dsc/10.1002-term.3131.pdf",
                    "BitstreamDescription": None,
                }
            ],
        }
    )


@pytest.fixture
def submission_message_body_for_update_operation():
    return json.dumps(
        {
            "Operation": "update",
            "SubmissionSystem": "DSpace@MIT",
            "CollectionHandle": None,
            "ItemHandle": "1721.1/123",
            "MetadataLocation": "s3://dsc/10.1002-term.3131.json",
            "Files": [
                {
                    "BitstreamName": "10.1002-term.3131.pdf",
                    "FileLocation": "s3://dsc/10.1002-term.3131.pdf",
                    "BitstreamDescription": None,
                }
            ],
        }
    )


def test_sqs_create_dss_message_attributes(sqs_client, submission_message_attributes):
    dss_message_attributes = sqs_client.create_dss_message_attributes(
        item_identifier="123",
        submission_source="Submission system",
        output_queue="DSS queue",
    )
    assert dss_message_attributes == submission_message_attributes


def test_sqs_create_dss_message_body_for_create_operation(
    sqs_client, submission_message_body_for_create_operation
):
    dss_message_body = sqs_client.create_dss_message_body(
        submission_system="DSpace@MIT",
        collection_handle="123.4/5678",
        metadata_s3_uri="s3://dsc/10.1002-term.3131.json",
        bitstream_s3_uris=["s3://dsc/10.1002-term.3131.pdf"],
    )
    assert dss_message_body == submission_message_body_for_create_operation


def test_sqs_create_dss_message_body_for_update_operation(
    sqs_client, submission_message_body_for_update_operation
):
    dss_message_body = sqs_client.create_dss_message_body(
        operation="update",
        submission_system="DSpace@MIT",
        item_handle="1721.1/123",
        metadata_s3_uri="s3://dsc/10.1002-term.3131.json",
        bitstream_s3_uris=["s3://dsc/10.1002-term.3131.pdf"],
    )

    assert dss_message_body == submission_message_body_for_update_operation


def test_sqs_delete_success(
    mocked_sqs_output,
    sqs_client,
    result_message_attributes,
    result_message_body_success,
):
    sqs_client.send(
        message_attributes=result_message_attributes,
        message_body=result_message_body_success,
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
    mocked_sqs_input,
    sqs_client,
    submission_message_attributes,
    submission_message_body_for_create_operation,
):
    sqs_client.queue_name = "mock-input-queue"
    response = sqs_client.send(
        message_attributes=submission_message_attributes,
        message_body=submission_message_body_for_create_operation,
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == HTTPStatus.OK


def test_sqs_send_nonexistent_queue_raises_error(
    mocked_sqs_input,
    sqs_client,
    submission_message_attributes,
    submission_message_body_for_create_operation,
):
    sqs_client.queue_name = "nonexistent"
    with pytest.raises(ClientError) as exception_info:
        sqs_client.send(
            message_attributes=submission_message_attributes,
            message_body=submission_message_body_for_create_operation,
        )

    assert str(exception_info.value) == (
        "An error occurred (AWS.SimpleQueueService.NonExistentQueue) when "
        "calling the GetQueueUrl operation: The specified queue does not exist."
    )


def test_sqs_receive_success(
    mocked_sqs_output,
    sqs_client,
    result_message_attributes,
    result_message_body_success,
):
    sqs_client.send(
        message_attributes=result_message_attributes,
        message_body=result_message_body_success,
    )
    messages = sqs_client.receive()
    for message in messages:
        assert message["Body"] == str(result_message_body_success)
        assert message["MessageAttributes"] == result_message_attributes


def test_sqs_receive_nonexistent_queue_raises_error(mocked_sqs_output, sqs_client):
    sqs_client.queue_name = "nonexistent"

    with pytest.raises(ClientError) as exception_info:
        next(sqs_client.receive())

    assert str(exception_info.value) == (
        "An error occurred (AWS.SimpleQueueService.NonExistentQueue) when calling "
        "the GetQueueUrl operation: The specified queue does not exist."
    )
