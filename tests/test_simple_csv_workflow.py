from unittest.mock import patch


def test_simple_csv_workflow_item_metadata_iter_success(
    simple_csv_workflow_instance, mocked_s3_simple_csv, item_metadata
):
    metadata_iter = simple_csv_workflow_instance.item_metadata_iter(
        metadata_file="metadata.csv"
    )
    assert next(metadata_iter) == item_metadata


def test_simple_csv_workflow_get_item_identifier_success(
    simple_csv_workflow_instance, item_metadata
):
    assert simple_csv_workflow_instance.get_item_identifier(item_metadata)


@patch("dsc.utilities.aws.s3.S3Client.get_files_iter")
def test_simple_csv_get_bitstreams_uris_if_prefix_id_success(
    mock_s3_client_get_files_iter, simple_csv_workflow_instance
):
    mock_s3_client_get_files_iter.return_value = [
        "s3://dsc/simple_csv/folder/123_001.pdf",
        "s3://dsc/simple_csv/folder/123_002.pdf",
    ]

    assert simple_csv_workflow_instance.get_bitstream_uris(item_identifier="123") == [
        "s3://dsc/simple_csv/folder/123_001.pdf",
        "s3://dsc/simple_csv/folder/123_002.pdf",
    ]


@patch("dsc.utilities.aws.s3.S3Client.get_files_iter")
def test_simple_csv_get_bitstreams_uris_if_filename_id_success(
    mock_s3_client_get_files_iter, simple_csv_workflow_instance
):
    mock_s3_client_get_files_iter.return_value = [
        "s3://dsc/simple_csv/folder/123.pdf",
    ]

    assert simple_csv_workflow_instance.get_bitstream_uris(item_identifier="123.pdf") == [
        "s3://dsc/simple_csv/folder/123.pdf"
    ]
