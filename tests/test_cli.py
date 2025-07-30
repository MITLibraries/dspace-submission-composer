import json
from unittest.mock import patch

from freezegun import freeze_time

from dsc.cli import main
from dsc.db.models import ItemSubmissionDB, ItemSubmissionStatus


@patch("dsc.utilities.aws.s3.S3Client.files_iter")
def test_reconcile_simple_csv_success(
    mock_s3_client_files_iter,
    caplog,
    runner,
    simple_csv_workflow_instance,
    mocked_s3_simple_csv,
):
    mock_s3_client_files_iter.return_value = [
        "s3://dsc/simple_csv/batch-aaa/123_001.pdf",
        "s3://dsc/simple_csv/batch-aaa/123_002.pdf",
    ]
    result = runner.invoke(
        main,
        [
            "--workflow-name",
            "simple_csv",
            "--batch-id",
            "batch-aaa",
            "reconcile",
        ],
    )
    assert result.exit_code == 0
    assert (
        "Successfully reconciled bitstreams and metadata for all 1 item(s)" in caplog.text
    )
    assert "Total time elapsed" in caplog.text


@patch("dsc.utilities.aws.s3.S3Client.files_iter")
def test_reconcile_simple_csv_if_no_metadata_raise_error(
    mock_s3_client_files_iter,
    caplog,
    runner,
    simple_csv_workflow_instance,
    mocked_s3_simple_csv,
):
    mock_s3_client_files_iter.return_value = [
        "s3://dsc/simple_csv/batch-aaa/123_001.pdf",
        "s3://dsc/simple_csv/batch-aaa/123_002.pdf",
        "s3://dsc/simple_csv/batch-aaa/124_001.pdf",
    ]
    result = runner.invoke(
        main,
        [
            "--workflow-name",
            "simple_csv",
            "--batch-id",
            "batch-aaa",
            "reconcile",
        ],
    )
    assert result.exit_code == 1
    assert "Failed to reconcile bitstreams and metadata" in caplog.text


def test_reconcile_if_non_reconcile_workflow_raise_error(
    caplog, runner, base_workflow_instance
):
    result = runner.invoke(
        main,
        [
            "--workflow-name",
            "test",
            "--batch-id",
            "batch-aaa",
            "reconcile",
        ],
    )
    assert result.exit_code == 1
    assert isinstance(result.exception, TypeError)


@freeze_time("2025-01-01 09:00:00")
def test_submit_success(
    caplog,
    runner,
    mocked_s3,
    mocked_ses,
    mocked_sqs_input,
    mocked_item_submission_db,
    base_workflow_instance,
    s3_client,
):
    caplog.set_level("DEBUG")
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/123_01.pdf")
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/123_02.jpg")
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/789_01.pdf")
    ItemSubmissionDB.create(
        item_identifier="123",
        batch_id="batch-aaa",
        workflow_name="test",
        status=ItemSubmissionStatus.RECONCILE_SUCCESS,
    )
    ItemSubmissionDB.create(
        item_identifier="789",
        batch_id="batch-aaa",
        workflow_name="test",
        status=ItemSubmissionStatus.RECONCILE_SUCCESS,
    )

    expected_submission_summary = {"total": 2, "submitted": 2, "skipped": 0, "errors": 0}

    result = runner.invoke(
        main,
        [
            "--verbose",
            "--workflow-name",
            "test",
            "--batch-id",
            "batch-aaa",
            "submit",
            "--collection-handle",
            "123.4/5678",
            "--email-recipients",
            "test@test.test,test2@test.test",
        ],
    )
    assert result.exit_code == 0
    assert (
        "Submitting messages to the DSS input queue 'mock-input-queue' "
        "for batch 'batch-aaa'"
    ) in caplog.text
    assert "Preparing submission for item: 123" in caplog.text
    assert (
        "Metadata uploaded to S3: s3://dsc/test/batch-aaa/dspace_metadata/123_metadata.json"
        in caplog.text
    )
    assert "Preparing submission for item: 789" in caplog.text
    assert (
        "Metadata uploaded to S3: s3://dsc/test/batch-aaa/dspace_metadata/789_metadata.json"
        in caplog.text
    )
    assert json.dumps(expected_submission_summary) in caplog.text
    assert "Application exiting" in caplog.text
    assert "Total time elapsed" in caplog.text


@patch("dsc.db.models.ItemSubmissionDB.get")
def test_finalize_success(
    mock_item_submission_db_get,
    caplog,
    runner,
    mocked_item_submission_db,
    mocked_ses,
    mocked_sqs_input,
    mocked_sqs_output,
    base_workflow_instance,
    sqs_client,
    result_message_attributes,
    result_message_body,
):
    caplog.set_level("DEBUG")

    mock_item_submission_db_get.return_value = ItemSubmissionDB(
        batch_id="batch-aaa", item_identifier="10.1002/term.3131", workflow_name="test"
    )

    sqs_client.send(
        message_attributes=result_message_attributes,
        message_body=result_message_body,
    )

    expected_processing_summary = {
        "received_messages": 1,
        "ingest_success": 1,
        "ingest_failed": 0,
        "ingest_unknown": 0,
    }

    result = runner.invoke(
        main,
        [
            "--verbose",
            "--workflow-name",
            "test",
            "--batch-id",
            "batch-aaa",
            "finalize",
            "--email-recipients",
            "test@test.test,test2@test.test",
        ],
    )

    assert result.exit_code == 0
    assert (
        "Processing DSS result messages from the output queue 'mock-output-queue'"
        in caplog.text
    )
    assert "Receiving messages from the queue 'mock-output-queue'" in caplog.text
    assert "Retrieved message" in caplog.text
    assert "No messages remain in the queue 'mock-output-queue'" in caplog.text
    assert json.dumps(expected_processing_summary) in caplog.text
    assert "Logs sent to ['test@test.test', 'test2@test.test']" in caplog.text
    assert "Application exiting" in caplog.text
    assert "Total time elapsed" in caplog.text
