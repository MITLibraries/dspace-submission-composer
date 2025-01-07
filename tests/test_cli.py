from dsc.cli import main


def test_reconcile_success(caplog, runner, mocked_s3, base_workflow_instance, s3_client):
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
            "reconcile",
        ],
    )
    assert result.output == ""
    assert result.exit_code == 0
    assert "Item identifiers and bitstreams matched: ['123']" in caplog.text
    assert "No bitstreams found for these item identifiers: {'789'}" in caplog.text
    assert "No item identifiers found for these bitstreams: {'456'}" in caplog.text
    assert "Total time elapsed" in caplog.text
