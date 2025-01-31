# ruff: noqa: SLF001
import json
from unittest.mock import patch

import pytest

from dsc.exceptions import ReconcileError


@patch("dsc.utilities.aws.s3.S3Client.files_iter")
def test_simple_csv_workflow_reconcile_bitstreams_and_metadata_success(
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
        "Successfully reconciled bitstreams and metadata for all items (n=1)"
    ) in caplog.text


@patch("dsc.utilities.aws.s3.S3Client.files_iter")
def test_simple_csv_workflow_reconcile_bitstreams_and_metadata_if_no_metadata_raise_error(
    mock_s3_client_files_iter,
    caplog,
    simple_csv_workflow_instance,
    mocked_s3_simple_csv,
):
    mock_s3_client_files_iter.return_value = [
        "s3://dsc/simple_csv/batch-aaa/123_001.pdf",
        "s3://dsc/simple_csv/batch-aaa/123_002.pdf",
        "s3://dsc/simple_csv/batch-aaa/124_001.pdf",
    ]
    expected_reconcile_error_message = {
        "note": "Failed to reconcile bitstreams and metadata.",
        "bitstreams_without_metadata": {
            "count": 1,
            "identifiers": ["124"],
        },
        "metadata_without_bitstreams": {
            "count": 0,
            "identifiers": [],
        },
    }
    with pytest.raises(ReconcileError):
        simple_csv_workflow_instance.reconcile_bitstreams_and_metadata()

    assert json.dumps(expected_reconcile_error_message) in caplog.text


@patch("dsc.utilities.aws.s3.S3Client.files_iter")
def test_simple_csv_workflow_get_item_identifiers_from_bitstreams_success(
    mock_s3_client_files_iter, simple_csv_workflow_instance
):
    mock_s3_client_files_iter.return_value = [
        "s3://dsc/simple_csv/batch-aaa/123_001.pdf",
        "s3://dsc/simple_csv/batch-aaa/123_002.pdf",
        "s3://dsc/simple_csv/batch-aaa/124_001.pdf",
    ]
    assert simple_csv_workflow_instance._get_item_identifiers_from_bitstreams() == {
        "123",
        "124",
    }


def test_simple_csv_workflow_get_item_identifiers_from_metadata_success(
    simple_csv_workflow_instance, mocked_s3_simple_csv
):
    assert simple_csv_workflow_instance._get_item_identifiers_from_metadata() == {"123"}


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
    assert simple_csv_workflow_instance.get_item_identifier(item_metadata) == "123"


@patch("dsc.utilities.aws.s3.S3Client.files_iter")
def test_simple_csv_get_bitstreams_uris_if_prefix_id_success(
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
def test_simple_csv_get_bitstreams_uris_if_filename_id_success(
    mock_s3_client_files_iter, simple_csv_workflow_instance
):
    mock_s3_client_files_iter.return_value = [
        "s3://dsc/simple_csv/batch-aaa/123.pdf",
    ]

    assert simple_csv_workflow_instance.get_bitstream_s3_uris(
        item_identifier="123.pdf"
    ) == ["s3://dsc/simple_csv/batch-aaa/123.pdf"]
