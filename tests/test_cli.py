from dsc.cli import main


def test_reconcile_with_file_type_success(
    caplog, runner, mocked_s3, base_workflow_instance, s3_client
):
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/123_01.pdf")
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/123_02.pdf")
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/456_01.pdf")
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/789_01.jpg")
    result = runner.invoke(
        main,
        [
            "--workflow_name",
            "test",
            "--batch_id",
            "batch-aaa",
            "--collection_handle",
            "123.4/5678",
            "reconcile",
            "--file_type",
            "pdf",
        ],
    )
    assert result.exit_code == 0
    assert "Item identifiers and bitstreams successfully matched: ['123']" in caplog.text
    assert (
        "No bitstreams found for the following item identifiers: {'789'}" in caplog.text
    )
    assert (
        "No item identifiers found for the following bitstreams: {'456'}" in caplog.text
    )
    assert "Total time elapsed" in caplog.text


def test_reconcile_without_file_type_success(
    caplog, runner, mocked_s3, base_workflow_instance, s3_client
):
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/123_01.pdf")
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/123_02.pdf")
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/456_01.pdf")
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/789_01.jpg")
    result = runner.invoke(
        main,
        [
            "--workflow_name",
            "test",
            "--batch_id",
            "batch-aaa",
            "--collection_handle",
            "123.4/5678",
            "reconcile",
        ],
    )
    assert result.exit_code == 0
    assert (
        "Item identifiers and bitstreams successfully matched: ['123', '789']"
        in caplog.text
    )
    assert (
        "No item identifiers found for the following bitstreams: {'456'}" in caplog.text
    )
    assert "Total time elapsed" in caplog.text
