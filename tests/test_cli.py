from unittest.mock import patch

from dsc.cli import main


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
    assert "Successfully reconciled bitstreams and metadata for all items" in caplog.text
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


def test_submit_success(
    caplog, runner, mocked_s3, mocked_sqs_input, base_workflow_instance, s3_client
):
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/123_01.pdf")
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/123_02.jpg")
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/456_01.pdf")
    caplog.set_level("DEBUG")
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
            "test@test.edu",
        ],
    )
    assert result.exit_code == 0
    assert "Beginning submission of batch ID: batch-aaa" in caplog.text
    assert "Processing submission for '123'" in caplog.text
    assert (
        "Metadata uploaded to S3: s3://dsc/test/batch-aaa/123_metadata.json"
        in caplog.text
    )
    assert "Processing submission for '789'" in caplog.text
    assert (
        "Metadata uploaded to S3: s3://dsc/test/batch-aaa/789_metadata.json"
        in caplog.text
    )
    assert "Total time elapsed" in caplog.text


def test_finalize_success(
    caplog,
    runner,
    mocked_ses,
    mocked_sqs_input,
    mocked_sqs_output,
    base_workflow_instance,
    sqs_client,
    result_message_attributes,
    result_message_body,
):
    caplog.set_level("DEBUG")
    sqs_client.send(
        message_attributes=result_message_attributes,
        message_body=result_message_body,
    )
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
    assert "Parsed message" in caplog.text
    assert "No messages remain in the queue 'mock-output-queue'" in caplog.text
    assert "Logs sent to ['test@test.test', 'test2@test.test']" in caplog.text
    assert "Application exiting" in caplog.text
    assert "Total time elapsed" in caplog.text
