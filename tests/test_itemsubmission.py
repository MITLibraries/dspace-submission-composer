from datetime import UTC, datetime
from http import HTTPStatus
from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError
from freezegun import freeze_time

from dsc.db.models import ItemSubmissionDB, ItemSubmissionStatus
from dsc.item_submission import ItemSubmission


def test_itemsubmission_init_success(item_submission_instance, dspace_metadata):
    assert item_submission_instance.dspace_metadata == dspace_metadata
    assert item_submission_instance.bitstream_s3_uris == [
        "s3://dsc/workflow/folder/123_01.pdf",
        "s3://dsc/workflow/folder/123_02.pdf",
    ]
    assert item_submission_instance.item_identifier == "123"


def test_from_metadata_success():
    test_metadata = {"item_identifier": "test-123"}
    item = ItemSubmission.from_metadata(
        batch_id="batch-001", item_metadata=test_metadata, workflow_name="test-workflow"
    )

    assert item == ItemSubmission(
        batch_id="batch-001",
        item_identifier="test-123",
        workflow_name="test-workflow",
        collection_handle=None,
        last_run_date=None,
        submit_attempts=0,
        ingest_attempts=0,
        ingest_date=None,
        last_result_message=None,
        dspace_handle=None,
        status=None,
        status_details=None,
        dspace_metadata=None,
        bitstream_s3_uris=None,
        metadata_s3_uri="",
    )


def test_from_batch_id_and_item_identifier_success(mocked_item_submission_db):
    item_submission_db = ItemSubmissionDB(
        batch_id="batch-001",
        item_identifier="test-123",
        workflow_name="test-workflow",
    )
    item_submission_db.save()
    item = ItemSubmission.from_batch_id_and_item_identifier("batch-001", "test-123")

    assert item == ItemSubmission(
        batch_id="batch-001",
        item_identifier="test-123",
        workflow_name="test-workflow",
        collection_handle=None,
        last_run_date=None,
        submit_attempts=0,
        ingest_attempts=0,
        ingest_date=None,
        last_result_message=None,
        dspace_handle=None,
        status=None,
        status_details=None,
        dspace_metadata=None,
        bitstream_s3_uris=None,
        metadata_s3_uri="",
    )


@freeze_time("2025-01-01 09:00:00")
def test_from_db_success(mocked_item_submission_db):
    run_date = datetime.now(UTC)
    item_submission_db = ItemSubmissionDB(
        batch_id="batch-001",
        item_identifier="test-123",
        workflow_name="test-workflow",
        collection_handle="123.4/5678",
        dspace_handle="dspace-123",
        status=ItemSubmissionStatus.RECONCILE_SUCCESS,
        status_details=None,
        ingest_attempts=1,
        last_result_message=None,
        submit_attempts=1,
        ingest_date=run_date,
        last_run_date=run_date,
    )
    item_submission_db.save()
    item = ItemSubmission.from_db(item_submission_db)

    assert item == ItemSubmission(
        batch_id="batch-001",
        item_identifier="test-123",
        workflow_name="test-workflow",
        collection_handle="123.4/5678",
        last_run_date=run_date,
        submit_attempts=1,
        ingest_attempts=1,
        ingest_date=run_date,
        last_result_message=None,
        dspace_handle="dspace-123",
        status=ItemSubmissionStatus.RECONCILE_SUCCESS,
        status_details=None,
        dspace_metadata=None,
        bitstream_s3_uris=None,
        metadata_s3_uri="",
    )


def test_update_db_success(item_submission_instance, mocked_item_submission_db):
    item_submission_instance.status = ItemSubmissionStatus.RECONCILE_SUCCESS
    item_submission_instance.status_details = "Test update"
    item_submission_instance.submit_attempts = 1

    item_submission_instance.update_db()

    record = ItemSubmissionDB.get(
        hash_key=item_submission_instance.batch_id,
        range_key=item_submission_instance.item_identifier,
    )
    assert record.status == ItemSubmissionStatus.RECONCILE_SUCCESS
    assert record.status_details == "Test update"
    assert record.submit_attempts == 1
    assert record.workflow_name == item_submission_instance.workflow_name


def test_update_db_excludes_metadata_and_bitstreams(
    item_submission_instance, mocked_item_submission_db
):
    item_submission_instance.dspace_metadata = {"title": "Test Item"}
    item_submission_instance.bitstream_s3_uris = [
        "s3://dsc/workflow/folder/123_01.pdf",
        "s3://dsc/workflow/folder/123_02.pdf",
    ]
    item_submission_instance.update_db()
    record = ItemSubmissionDB.get(
        hash_key=item_submission_instance.batch_id,
        range_key=item_submission_instance.item_identifier,
    )
    assert hasattr(record, "dspace_metadata") is False
    assert hasattr(record, "bitstream_s3_uris") is False


def test_ready_to_submit_with_none_status(item_submission_instance):
    item_submission_instance.status = None
    assert item_submission_instance.ready_to_submit() is False


def test_ready_to_submit_with_reconcile_failed(item_submission_instance):
    item_submission_instance.status = ItemSubmissionStatus.RECONCILE_FAILED
    assert item_submission_instance.ready_to_submit() is False


def test_ready_to_submit_with_ingest_success(item_submission_instance):
    item_submission_instance.status = ItemSubmissionStatus.INGEST_SUCCESS
    assert item_submission_instance.ready_to_submit() is False


def test_ready_to_submit_with_submit_success(item_submission_instance):
    item_submission_instance.status = ItemSubmissionStatus.SUBMIT_SUCCESS
    assert item_submission_instance.ready_to_submit() is False


def test_ready_to_submit_with_max_retries(item_submission_instance):
    item_submission_instance.status = ItemSubmissionStatus.MAX_RETRIES_REACHED
    assert item_submission_instance.ready_to_submit() is False


def test_ready_to_submit_with_reconcile_success(item_submission_instance):
    item_submission_instance.status = ItemSubmissionStatus.RECONCILE_SUCCESS
    assert item_submission_instance.ready_to_submit() is True


def test_ready_to_submit_with_submit_failed(item_submission_instance):
    item_submission_instance.status = ItemSubmissionStatus.SUBMIT_FAILED
    assert item_submission_instance.ready_to_submit() is True


def test_ready_to_submit_with_ingest_failed(item_submission_instance):
    item_submission_instance.status = ItemSubmissionStatus.INGEST_FAILED
    assert item_submission_instance.ready_to_submit() is True


def test_upload_dspace_metadata_success(mocked_s3, item_submission_instance, s3_client):
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
def test_upload_dspace_metadata_handles_exception(
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
    with pytest.raises(ClientError):
        item_submission_instance.upload_dspace_metadata(
            bucket="dsc", prefix="test/batch-bbb/"
        )

    assert item_submission_instance.status == ItemSubmissionStatus.SUBMIT_FAILED
    assert (
        "The specified bucket does not exist" in item_submission_instance.status_details
    )
    assert item_submission_instance.submit_attempts == 1
    assert item_submission_instance.metadata_s3_uri == ""


def test_send_submission_message(
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


def test_send_submission_message_raises_value_error(
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


def test_send_submission_message_handles_exception(
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
        with pytest.raises(ClientError):
            item_submission_instance.send_submission_message(
                "workflow", "mock-output-queue", "DSpace@MIT", "1234/5678"
            )

    assert item_submission_instance.status == ItemSubmissionStatus.SUBMIT_FAILED
    assert "The specified queue does not exist" in item_submission_instance.status_details
    assert item_submission_instance.submit_attempts == 1
