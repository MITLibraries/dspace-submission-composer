# ruff: noqa: SLF001
import json
from unittest.mock import patch


@patch("dsc.utilities.aws.s3.S3Client.files_iter")
def test_workflow_simple_csv_reconcile_bitstreams_and_metadata_success(
    mock_s3_client_files_iter,
    caplog,
    simple_csv_workflow_instance,
    mocked_s3_simple_csv,
):
    mock_s3_client_files_iter.return_value = [
        "s3://dsc/simple_csv/batch-aaa/123_001.pdf",
        "s3://dsc/simple_csv/batch-aaa/123_002.pdf",
    ]
    simple_csv_workflow_instance.reconcile_bitstreams_and_metadata()
    assert (
        "Successfully reconciled bitstreams and metadata for all 1 item(s)"
    ) in caplog.text


@patch("dsc.utilities.aws.s3.S3Client.files_iter")
def test_workflow_simple_csv_reconcile_bitstreams_and_metadata_if_no_metadata_success(
    mock_s3_client_files_iter,
    caplog,
    simple_csv_workflow_instance,
    mocked_s3_simple_csv,
):
    mock_s3_client_files_iter.return_value = [
        "s3://dsc/simple_csv/batch-aaa/124_001.pdf",
    ]
    expected_reconcile_summary = {
        "reconciled": 0,
        "bitstreams_without_metadata": 1,
        "metadata_without_bitstreams": 1,
    }

    reconciled = simple_csv_workflow_instance.reconcile_bitstreams_and_metadata()

    assert not reconciled
    assert f"Reconcile results: {json.dumps(expected_reconcile_summary)}" in caplog.text
    assert "Failed to reconcile bitstreams and metadata" in caplog.text
    assert (
        "Bitstreams without metadata: ['s3://dsc/simple_csv/batch-aaa/124_001.pdf']"
    ) in caplog.text
    assert "Metadata without bitstreams: ['123']" in caplog.text


def test_workflow_simple_csv_match_metadata_to_bitstreams(simple_csv_workflow_instance):
    assert simple_csv_workflow_instance._match_metadata_to_bitstreams(
        item_identifiers=["123"],
        bitstream_filenames=[
            "s3://dsc/simple_csv/batch-aaa/123_001.pdf",
            "s3://dsc/simple_csv/batch-aaa/123_002.pdf",
            "s3://dsc/simple_csv/batch-aaa/124_001.pdf",
        ],
    ) == {
        "123": [
            "s3://dsc/simple_csv/batch-aaa/123_001.pdf",
            "s3://dsc/simple_csv/batch-aaa/123_002.pdf",
        ]
    }


def test_workflow_simple_csv_get_item_identifiers_from_metadata_success(
    simple_csv_workflow_instance, mocked_s3_simple_csv
):
    assert simple_csv_workflow_instance._get_item_identifiers_from_metadata() == {"123"}


def test_workflow_simple_csv_item_metadata_iter_success(
    simple_csv_workflow_instance, mocked_s3_simple_csv, item_metadata
):
    metadata_iter = simple_csv_workflow_instance.item_metadata_iter(
        metadata_file="metadata.csv"
    )
    assert next(metadata_iter) == item_metadata


def test_workflow_simple_csv_get_item_identifier_success(
    simple_csv_workflow_instance, item_metadata
):
    assert simple_csv_workflow_instance.get_item_identifier(item_metadata) == "123"


@patch("dsc.utilities.aws.s3.S3Client.files_iter")
def test_workflow_simple_csv_get_bitstreams_uris_if_prefix_id_success(
    mock_s3_client_files_iter, simple_csv_workflow_instance
):
    mock_s3_client_files_iter.return_value = [
        "s3://dsc/simple_csv/batch-aaa/123_001.pdf",
        "s3://dsc/simple_csv/batch-aaa/123_002.pdf",
    ]

    assert simple_csv_workflow_instance.get_bitstream_s3_uris(item_identifier="123") == [
        "s3://dsc/simple_csv/batch-aaa/123_001.pdf",
        "s3://dsc/simple_csv/batch-aaa/123_002.pdf",
    ]


@patch("dsc.utilities.aws.s3.S3Client.files_iter")
def test_workflow_simple_csv_get_bitstreams_uris_if_filename_id_success(
    mock_s3_client_files_iter, simple_csv_workflow_instance
):
    mock_s3_client_files_iter.return_value = [
        "s3://dsc/simple_csv/batch-aaa/123.pdf",
    ]

    assert simple_csv_workflow_instance.get_bitstream_s3_uris(
        item_identifier="123.pdf"
    ) == ["s3://dsc/simple_csv/batch-aaa/123.pdf"]
