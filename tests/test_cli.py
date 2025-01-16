from dsc.cli import main


def test_reconcile_success(caplog, runner, mocked_s3, base_workflow_instance, s3_client):
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/123_01.pdf")
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/123_02.jpg")
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/789_01.pdf")
    result = runner.invoke(
        main,
        [
            "--workflow-name",
            "test",
            "--collection-handle",
            "123.4/5678",
            "--batch-id",
            "batch-aaa",
            "--email-recipients",
            "test@test.test",
            "reconcile",
        ],
    )
    assert result.exit_code == 0
    assert (
        "Item identifiers from batch metadata with matching bitstreams: ['123', '789']"
        in caplog.text
    )
    assert "All item identifiers and bitstreams successfully matched" in caplog.text
    assert "Total time elapsed" in caplog.text


def test_reconcile_discrepancies_logged(
    caplog, runner, mocked_s3, base_workflow_instance, s3_client
):
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/123_01.pdf")
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/123_02.jpg")
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/456_01.pdf")
    result = runner.invoke(
        main,
        [
            "--workflow-name",
            "test",
            "--collection-handle",
            "123.4/5678",
            "--batch-id",
            "batch-aaa",
            "--email-recipients",
            "test@test.edu",
            "reconcile",
        ],
    )
    assert result.exit_code == 0
    assert (
        "Item identifiers from batch metadata with matching bitstreams: ['123']"
        in caplog.text
    )
    assert "No bitstreams found for these item identifiers: {'789'}" in caplog.text
    assert "No item identifiers found for these bitstreams: {'456'}" in caplog.text
    assert "Total time elapsed" in caplog.text


def test_deposit_success(
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
            "--collection-handle",
            "123.4/5678",
            "--batch-id",
            "batch-aaa",
            "--email-recipients",
            "test@test.edu",
            "deposit",
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
            "--collection-handle",
            "123.4/5678",
            "--batch-id",
            "batch-aaa",
            "--output-queue",
            "mock-output-queue",
            "--email-recipients",
            "test@test.test,test2@test.test",
            "finalize",
        ],
    )
    assert result.exit_code == 0
    assert "Processing result messages in 'mock-output-queue'" in caplog.text
    assert "Receiving messages from SQS queue: 'mock-output-queue'" in caplog.text
    assert "Message retrieved from SQS queue 'mock-output-queue':" in caplog.text
    assert "No more messages from SQS queue: 'mock-output-queue'" in caplog.text
    assert "Messages received and deleted from 'mock-output-queue'" in caplog.text
    assert "Logs sent to ['test@test.test', 'test2@test.test']" in caplog.text
    assert "Application exiting" in caplog.text
    assert "Total time elapsed" in caplog.text
