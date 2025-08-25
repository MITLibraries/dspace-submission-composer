from http import HTTPStatus
from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError

from dsc.db.models import ItemSubmissionDB, ItemSubmissionStatus
from dsc.exceptions import (
    DSpaceMetadataUploadError,
    InvalidDSpaceMetadataError,
    ItemMetadatMissingRequiredFieldError,
    SQSMessageSendError,
)
from dsc.item_submission import ItemSubmission


def test_itemsubmission_init_success(item_submission_instance, dspace_metadata):
    assert item_submission_instance.dspace_metadata == dspace_metadata
    assert item_submission_instance.bitstream_s3_uris == [
        "s3://dsc/workflow/folder/123_01.pdf",
        "s3://dsc/workflow/folder/123_02.pdf",
    ]
    assert item_submission_instance.item_identifier == "123"


def test_itemsubmission_get_success(mocked_item_submission_db):
    # create record in item submissions DynamoDB table
    ItemSubmissionDB.create(
        batch_id="batch-aaa", item_identifier="123", workflow_name="test"
    )

    assert ItemSubmission.get(
        batch_id="batch-aaa", item_identifier="123"
    ) == ItemSubmission(batch_id="batch-aaa", item_identifier="123", workflow_name="test")


def test_itemsubmission_get_if_does_not_exist_in_db_success(mocked_item_submission_db):
    assert ItemSubmission.get(batch_id="batch-aaa", item_identifier="123") is None


def test_itemsubmission_get_batch_success(mocked_item_submission_db):
    # create records in item submissions DynamoDB table
    ItemSubmissionDB.create(
        batch_id="batch-aaa", item_identifier="123", workflow_name="test"
    )
    # create record in item submissions DynamoDB table
    ItemSubmissionDB.create(
        batch_id="batch-aaa", item_identifier="456", workflow_name="test"
    )

    assert list(ItemSubmission.get_batch(batch_id="batch-aaa")) == [
        ItemSubmission(batch_id="batch-aaa", item_identifier="123", workflow_name="test"),
        ItemSubmission(batch_id="batch-aaa", item_identifier="456", workflow_name="test"),
    ]


def test_itemsubmission_create_success():
    assert ItemSubmission.create(
        batch_id="batch-aaa",
        item_identifier="123",
        workflow_name="test",
        source_system_identifier="abcde",
    ) == ItemSubmission(
        batch_id="batch-aaa",
        item_identifier="123",
        workflow_name="test",
        source_system_identifier="abcde",
    )


def test_itemsubmission_upsert_db_success(
    item_submission_instance, mocked_item_submission_db
):
    item_submission_instance.status = ItemSubmissionStatus.RECONCILE_SUCCESS
    item_submission_instance.status_details = "Test update"
    item_submission_instance.submit_attempts = 1

    item_submission_instance.upsert_db()

    record = ItemSubmissionDB.get(
        hash_key=item_submission_instance.batch_id,
        range_key=item_submission_instance.item_identifier,
    )
    assert record.status == ItemSubmissionStatus.RECONCILE_SUCCESS
    assert record.status_details == "Test update"
    assert record.submit_attempts == 1
    assert record.workflow_name == item_submission_instance.workflow_name


def test_itemsubmission_upsert_db_excludes_metadata_and_bitstreams(
    item_submission_instance, mocked_item_submission_db
):
    item_submission_instance.dspace_metadata = {"title": "Test Item"}
    item_submission_instance.bitstream_s3_uris = [
        "s3://dsc/workflow/folder/123_01.pdf",
        "s3://dsc/workflow/folder/123_02.pdf",
    ]
    item_submission_instance.upsert_db()
    record = ItemSubmissionDB.get(
        hash_key=item_submission_instance.batch_id,
        range_key=item_submission_instance.item_identifier,
    )
    assert hasattr(record, "dspace_metadata") is False
    assert hasattr(record, "bitstream_s3_uris") is False


def test_exceeded_retry_threshold_false(item_submission_instance):
    item_submission_instance.ingest_attempts = 17
    item_submission_instance.status = ItemSubmissionStatus.SUBMIT_SUCCESS
    item_submission_instance._exceeded_retry_threshold()  # noqa: SLF001
    assert item_submission_instance.status == ItemSubmissionStatus.SUBMIT_SUCCESS


def test_exceeded_retry_threshold_true(item_submission_instance):
    item_submission_instance.ingest_attempts = 21
    item_submission_instance.status = ItemSubmissionStatus.SUBMIT_SUCCESS
    item_submission_instance._exceeded_retry_threshold()  # noqa: SLF001
    assert item_submission_instance.status == ItemSubmissionStatus.MAX_RETRIES_REACHED


def test_itemsubmission_ready_to_submit_with_none_status(item_submission_instance):
    item_submission_instance.status = None
    assert item_submission_instance.ready_to_submit() is False


def test_itemsubmission_ready_to_submit_with_reconcile_failed(item_submission_instance):
    item_submission_instance.status = ItemSubmissionStatus.RECONCILE_FAILED
    assert item_submission_instance.ready_to_submit() is False


def test_itemsubmission_ready_to_submit_with_ingest_success(item_submission_instance):
    item_submission_instance.status = ItemSubmissionStatus.INGEST_SUCCESS
    assert item_submission_instance.ready_to_submit() is False


def test_itemsubmission_ready_to_submit_with_submit_success(item_submission_instance):
    item_submission_instance.status = ItemSubmissionStatus.SUBMIT_SUCCESS
    assert item_submission_instance.ready_to_submit() is False


def test_itemsubmission_ready_to_submit_with_max_retries(item_submission_instance):
    item_submission_instance.status = ItemSubmissionStatus.MAX_RETRIES_REACHED
    assert item_submission_instance.ready_to_submit() is False


def test_itemsubmission_ready_to_submit_with_reconcile_success(item_submission_instance):
    item_submission_instance.status = ItemSubmissionStatus.RECONCILE_SUCCESS
    assert item_submission_instance.ready_to_submit() is True


def test_itemsubmission_ready_to_submit_with_submit_failed(item_submission_instance):
    item_submission_instance.status = ItemSubmissionStatus.SUBMIT_FAILED
    assert item_submission_instance.ready_to_submit() is True


def test_itemsubmission_ready_to_submit_with_ingest_failed(item_submission_instance):
    item_submission_instance.status = ItemSubmissionStatus.INGEST_FAILED
    assert item_submission_instance.ready_to_submit() is True


def test_itemsubmission_create_dspace_metadata_success(
    item_submission_instance, item_metadata, metadata_mapping
):
    item_metadata["topics"] = [
        "Topic Header - Topic Subheading - Topic Name",
        "Topic Header 2 - Topic Subheading 2 - Topic Name 2",
    ]
    item_submission_instance.create_dspace_metadata(item_metadata, metadata_mapping)
    assert item_submission_instance.dspace_metadata == {
        "metadata": [
            {"key": "dc.title", "language": "en_US", "value": "Title"},
            {"key": "dc.contributor", "language": None, "value": "Author 1"},
            {"key": "dc.contributor", "language": None, "value": "Author 2"},
            {
                "key": "dc.subject",
                "language": None,
                "value": "Topic Header - Topic Subheading - Topic Name",
            },
            {
                "key": "dc.subject",
                "language": None,
                "value": "Topic Header 2 - Topic Subheading 2 - Topic Name 2",
            },
        ]
    }


def test_itemsubmission_create_dspace_metadata_required_field_missing_raises_exception(
    item_submission_instance, item_metadata, metadata_mapping
):
    item_metadata.pop("title")
    with pytest.raises(ItemMetadatMissingRequiredFieldError):
        item_submission_instance.create_dspace_metadata(item_metadata, metadata_mapping)


def test_itemsubmission_validate_dspace_metadata_success(
    item_submission_instance,
    dspace_metadata,
):
    item_submission_instance.dspace_metadata = dspace_metadata
    assert item_submission_instance.validate_dspace_metadata()


def test_itemsubmission_validate_dspace_metadata_invalid_raises_exception(
    item_submission_instance,
):
    item_submission_instance.dspace_metadata = {}
    with pytest.raises(InvalidDSpaceMetadataError):
        item_submission_instance.validate_dspace_metadata()


def test_itemsubmission_upload_dspace_metadata_success(
    mocked_s3, item_submission_instance, s3_client
):
    item_submission_instance.upload_dspace_metadata("dsc", "workflow/folder/")
    assert (
        item_submission_instance.metadata_s3_uri
        == "s3://dsc/workflow/folder/dspace_metadata/123_metadata.json"
    )
    response = s3_client.client.get_object(
        Bucket="dsc", Key="workflow/folder/dspace_metadata/123_metadata.json"
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == HTTPStatus.OK


@patch("dsc.utilities.aws.s3.S3Client.put_file")
def test_itemsubmission_upload_dspace_metadata_raises_custom_exception(
    mock_put_file, item_submission_instance, mocked_item_submission_db
):
    mock_put_file.side_effect = ClientError(
        operation_name="PutObject",
        error_response={
            "Error": {
                "Code": "NoSuchBucket",
                "Message": "The specified bucket does not exist",
            }
        },
    )
    with pytest.raises(
        DSpaceMetadataUploadError, match="The specified bucket does not exist"
    ):
        item_submission_instance.upload_dspace_metadata(
            bucket="dsc", prefix="test/batch-bbb/"
        )


def test_itemsubmission_send_submission_message(
    mocked_sqs_input, mocked_sqs_output, item_submission_instance
):
    item_submission_instance.metadata_s3_uri = (
        "s3://dsc/workflow/folder/123_metadata.json"
    )
    item_submission_instance.bitstream_s3_uris = [
        "s3://dsc/workflow/folder/123_01.pdf",
        "s3://dsc/workflow/folder/123_02.pdf",
    ]
    response = item_submission_instance.send_submission_message(
        "workflow",
        "mock-output-queue",
        "DSpace@MIT",
        "1234/5678",
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == HTTPStatus.OK


def test_itemsubmission_send_submission_message_raises_value_error(
    mocked_sqs_input, mocked_sqs_output, item_submission_instance
):
    item_submission_instance.metadata_s3_uri = ""
    item_submission_instance.bitstream_s3_uris = []

    assert not item_submission_instance.status
    assert item_submission_instance.submit_attempts == 0

    with pytest.raises(
        ValueError, match="Metadata S3 URI or bitstream S3 URIs not set for item: 123"
    ):
        item_submission_instance.send_submission_message(
            "workflow", "mock-output-queue", "DSpace@MIT", "1234/5678"
        )


def test_itemsubmission_send_submission_message_raises_custom_exception(
    mocked_sqs_input,
    mocked_sqs_output,
    mocked_item_submission_db,
    item_submission_instance,
):
    item_submission_instance.metadata_s3_uri = (
        "s3://dsc/workflow/folder/123_metadata.json"
    )
    item_submission_instance.bitstream_s3_uris = [
        "s3://dsc/workflow/folder/123_01.pdf",
        "s3://dsc/workflow/folder/123_02.pdf",
    ]
    with patch("dsc.utilities.aws.sqs.SQSClient.send") as mock_send:
        mock_send.side_effect = ClientError(
            operation_name="SendMessage",
            error_response={
                "Error": {
                    "Code": "QueueDoesNotExist",
                    "Message": "The specified queue does not exist",
                }
            },
        )
        with pytest.raises(
            SQSMessageSendError, match="The specified queue does not exist"
        ):
            item_submission_instance.send_submission_message(
                "workflow", "mock-output-queue", "DSpace@MIT", "1234/5678"
            )
