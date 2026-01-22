import json
from unittest.mock import MagicMock, patch

import boto3
import click
from freezegun import freeze_time

from dsc.cli import main
from dsc.db.models import ItemSubmissionDB, ItemSubmissionStatus


def test_create_success(
    caplog,
    runner,
    base_workflow_instance,
    mocked_item_submission_db,
    mocked_s3,
    s3_client,
):
    caplog.set_level("DEBUG")
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/123_001.pdf")
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/123_002.jpg")
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/789_001.pdf")

    result = runner.invoke(
        main,
        [
            "--verbose",
            "--workflow-name",
            "test",
            "--batch-id",
            "batch-aaa",
            "create",
        ],
    )

    assert result.exit_code == 0
    assert "Created record" in caplog.text
    assert "Saved record" in caplog.text


def test_create_with_sync_data_success(
    caplog,
    runner,
    base_workflow_instance,
    mocked_item_submission_db,
    mocked_s3,
    s3_client,
):
    caplog.set_level("DEBUG")
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/123_001.pdf")
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/123_002.jpg")
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/789_001.pdf")

    mock_sync_callback = MagicMock(name="sync_callback")

    with patch.object(main.commands["sync"], "callback", new=mock_sync_callback):
        result = runner.invoke(
            main,
            [
                "--verbose",
                "--workflow-name",
                "test",
                "--batch-id",
                "batch-aaa",
                "create",
                "--sync-data",
            ],
        )

        assert result.exit_code == 0
        mock_sync_callback.assert_called_once()
        assert "Created record" in caplog.text
        assert "Saved record" in caplog.text


def test_create_with_sync_data_error(
    caplog,
    runner,
    base_workflow_instance,
    mocked_item_submission_db,
    mocked_s3,
    s3_client,
):
    caplog.set_level("DEBUG")
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/123_001.pdf")
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/123_002.jpg")
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/789_001.pdf")

    mock_sync_callback = MagicMock(
        name="sync_callback", side_effect=click.exceptions.Exit(code=1)
    )

    with patch.object(main.commands["sync"], "callback", new=mock_sync_callback):
        result = runner.invoke(
            main,
            [
                "--verbose",
                "--workflow-name",
                "test",
                "--batch-id",
                "batch-aaa",
                "create",
                "--sync-data",
            ],
        )

        assert result.exit_code == 1
        mock_sync_callback.assert_called_once()
        assert "Failed to sync data, cannot proceed with batch creation" in caplog.text


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
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/123_001.pdf")
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/123_002.jpg")
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/789_001.pdf")
    ItemSubmissionDB(
        item_identifier="123",
        batch_id="batch-aaa",
        workflow_name="test",
        status=ItemSubmissionStatus.BATCH_CREATED,
    ).create()
    ItemSubmissionDB(
        item_identifier="789",
        batch_id="batch-aaa",
        workflow_name="test",
        status=ItemSubmissionStatus.BATCH_CREATED,
    ).create()

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


def test_finalize_success(
    caplog,
    runner,
    mocked_item_submission_db,
    mocked_ses,
    mocked_sqs_input,
    mocked_sqs_output,
    base_workflow_instance,
    sqs_client,
    result_message_attributes,
    result_message_body_success,
):
    caplog.set_level("DEBUG")

    ItemSubmissionDB(
        item_identifier="10.1002/term.3131",
        batch_id="batch-aaa",
        workflow_name="test",
        status=ItemSubmissionStatus.SUBMIT_SUCCESS,
    ).create()

    sqs_client.send(
        message_attributes=result_message_attributes,
        message_body=result_message_body_success,
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


def test_sync_success(caplog, runner, monkeypatch, moto_server, config_instance):
    """Run sync using moto stand-alone server."""
    monkeypatch.setenv("S3_BUCKET_SUBMISSION_ASSETS", "destination")
    monkeypatch.setenv("S3_BUCKET_SYNC_SOURCE", "source")
    monkeypatch.setenv("AWS_ENDPOINT_URL", moto_server)

    # set fake AWS credentials
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

    # point boto3 client to test server
    s3 = boto3.client(
        "s3", region_name=config_instance.aws_region_name, endpoint_url=moto_server
    )

    # create 'source' bucket for syncing
    s3.create_bucket(Bucket="source")
    s3.put_object(
        Bucket="source",
        Key="test/batch-aaa/123.zip",
        Body=b"",
    )

    # create 'destination' bucket
    s3.create_bucket(Bucket="destination")

    result = runner.invoke(
        main,
        ["--verbose", "--workflow-name", "test", "--batch-id", "batch-aaa", "sync"],
    )

    assert result.exit_code == 0
    assert (
        "Syncing data from s3://source/test/batch-aaa/ to s3://destination/test/batch-aaa/"
        in caplog.text
    )
    assert (
        "copy: s3://source/test/batch-aaa/123.zip to s3://destination/test/batch-aaa/123.zip"
        in caplog.text
    )


def test_sync_use_source_and_destination_success(
    caplog, runner, monkeypatch, moto_server, config_instance
):
    """Run sync using moto stand-alone server."""
    monkeypatch.setenv("S3_BUCKET_SUBMISSION_ASSETS", "destination")
    monkeypatch.setenv("AWS_ENDPOINT_URL", moto_server)

    # set fake AWS credentials
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

    # point boto3 client to test server
    s3 = boto3.client(
        "s3", region_name=config_instance.aws_region_name, endpoint_url=moto_server
    )

    # create 'source' bucket for syncing
    s3.create_bucket(Bucket="source")
    s3.put_object(
        Bucket="source",
        Key="test/batch-aaa/123.zip",
        Body=b"",
    )

    # create 'destination' bucket
    s3.create_bucket(Bucket="destination")

    result = runner.invoke(
        main,
        [
            "--verbose",
            "--workflow-name",
            "test",
            "--batch-id",
            "batch-aaa",
            "sync",
            "--source",
            "s3://source/test/batch-aaa",
            "--destination",
            "s3://destination/test/batch-aaa",
        ],
    )

    assert result.exit_code == 0
    assert (
        "Syncing data from s3://source/test/batch-aaa to s3://destination/test/batch-aaa"
        in caplog.text
    )
    assert (
        "copy: s3://source/test/batch-aaa/123.zip to s3://destination/test/batch-aaa/123.zip"
        in caplog.text
    )


def test_sync_bad_usage_raise_error(
    caplog, runner, monkeypatch, moto_server, config_instance
):
    """Run sync using moto stand-alone server."""
    monkeypatch.setenv("S3_BUCKET_SUBMISSION_ASSETS", "destination")
    monkeypatch.setenv("AWS_ENDPOINT_URL", moto_server)

    # set fake AWS credentials
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

    # point boto3 client to test server
    s3 = boto3.client(
        "s3", region_name=config_instance.aws_region_name, endpoint_url=moto_server
    )

    # create 'source' bucket for syncing
    s3.create_bucket(Bucket="source")
    s3.put_object(
        Bucket="source",
        Key="test/batch-aaa/123.zip",
        Body=b"",
    )

    # create 'destination' bucket
    s3.create_bucket(Bucket="destination")

    result = runner.invoke(
        main,
        [
            "--verbose",
            "--workflow-name",
            "test",
            "--batch-id",
            "batch-aaa",
            "sync",
        ],
        catch_exceptions=True,
    )

    assert result.exit_code != 0
    assert (
        "Either provide '--source / -s' and '--destination / -d' or "
        "set the S3_BUCKET_SYNC_SOURCE environment variable"
    ) in result.output
