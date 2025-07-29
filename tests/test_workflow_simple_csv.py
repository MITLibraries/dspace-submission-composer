# ruff: noqa: SLF001
import csv
import io
import json
from io import StringIO
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
from freezegun import freeze_time
from pynamodb.exceptions import DoesNotExist

from dsc.db.models import ItemSubmissionDB, ItemSubmissionStatus


@freeze_time("2025-01-01 09:00:00")
@patch("dsc.utilities.aws.s3.S3Client.files_iter")
def test_workflow_simple_csv_reconcile_items_success(
    mock_s3_client_files_iter,
    caplog,
    simple_csv_workflow_instance,
    mocked_item_submission_db,
    mocked_s3_simple_csv,
):
    caplog.set_level("DEBUG")
    mock_s3_client_files_iter.return_value = [
        "s3://dsc/simple_csv/batch-aaa/123_001.pdf",
        "s3://dsc/simple_csv/batch-aaa/123_002.pdf",
    ]
    reconciled = simple_csv_workflow_instance.reconcile_items()
    item_submission_record = ItemSubmissionDB.get(hash_key="batch-aaa", range_key="123")
    assert reconciled
    assert "Item submission (item_identifier=123) reconciled" in caplog.text
    assert "Updating record" in caplog.text
    assert item_submission_record.status == ItemSubmissionStatus.RECONCILE_SUCCESS


@freeze_time("2025-01-01 09:00:00")
@patch("dsc.utilities.aws.s3.S3Client.files_iter")
def test_workflow_simple_csv_reconcile_items_if_item_submission_exists_success(
    mock_s3_client_files_iter,
    caplog,
    simple_csv_workflow_instance,
    mocked_item_submission_db,
    mocked_s3_simple_csv,
):
    caplog.set_level("DEBUG")
    mock_s3_client_files_iter.return_value = [
        "s3://dsc/simple_csv/batch-aaa/123_001.pdf",
        "s3://dsc/simple_csv/batch-aaa/123_002.pdf",
    ]

    # create record to force raise dsc.db.exceptions ItemSubmissionExistsError
    ItemSubmissionDB.create(
        batch_id="batch-aaa",
        item_identifier="123",
        workflow_name=simple_csv_workflow_instance.workflow_name,
        status=ItemSubmissionStatus.RECONCILE_SUCCESS,
    )

    reconciled = simple_csv_workflow_instance.reconcile_items()

    assert reconciled
    assert (
        "Record with primary keys batch_id=batch-aaa (hash key) and "
        "item_identifier=123 (range key) was previously reconciled, skipping update"
    ) in caplog.text


@freeze_time("2025-01-01 09:00:00")
@patch("dsc.utilities.aws.s3.S3Client.files_iter")
def test_workflow_simple_csv_reconcile_items_if_no_metadata_exclude_from_db(
    mock_s3_client_files_iter,
    caplog,
    simple_csv_workflow_instance,
    mocked_item_submission_db,
    mocked_s3_simple_csv,
):
    mock_s3_client_files_iter.return_value = [
        "s3://dsc/simple_csv/batch-aaa/123_001.pdf",
        "s3://dsc/simple_csv/batch-aaa/123_002.pdf",
        "s3://dsc/simple_csv/batch-aaa/456_003.pdf",
    ]

    # create record to force raise dsc.db.exceptions ItemSubmissionExistsError
    reconciled = simple_csv_workflow_instance.reconcile_items()
    assert not reconciled

    # since item identifiers are retrieved from SimpleCSV.item_metadata_iter
    # item identifiers associated with bitstreams without metadata are
    # NOT written to the DynamoDB table
    with pytest.raises(DoesNotExist):
        ItemSubmissionDB.get(hash_key="batch-aaa", range_key="456")


@freeze_time("2025-01-01 09:00:00")
@patch("dsc.utilities.aws.s3.S3Client.files_iter")
def test_workflow_simple_csv_reconcile_items_if_no_bitstreams_include_in_db(
    mock_s3_client_files_iter,
    caplog,
    simple_csv_workflow_instance,
    mocked_item_submission_db,
    mocked_s3_simple_csv,
):
    mock_s3_client_files_iter.return_value = [
        "s3://dsc/simple_csv/batch-aaa/123_001.pdf",
        "s3://dsc/simple_csv/batch-aaa/123_002.pdf",
    ]

    # update metadata CSV file
    csv_buffer = StringIO()
    writer = csv.DictWriter(
        csv_buffer, fieldnames=["title", "contributor", "item_identifier"]
    )
    writer.writeheader()
    writer.writerows(
        [
            {
                "title": "Title",
                "contributor": "Author 1|Author 2",
                "item_identifier": "123",
            },
            {
                "title": "Title",
                "contributor": "Author 1|Author 2",
                "item_identifier": "456",
            },
        ]
    )

    # seek to the beginning of the in-memory file before uploading
    csv_buffer.seek(0)

    mocked_s3_simple_csv.put_object(
        Bucket="dsc",
        Key="simple_csv/batch-aaa/metadata.csv",
        Body=csv_buffer.getvalue(),
    )

    # create record to force raise dsc.db.exceptions ItemSubmissionExistsError
    reconciled = simple_csv_workflow_instance.reconcile_items()
    assert not reconciled

    # since item identifiers are retrieved from SimpleCSV.item_metadata_iter
    # item identifiers associated with metadata without bitstreams
    # ARE written to the DynamoDB table
    record = ItemSubmissionDB.get(hash_key="batch-aaa", range_key="456")
    assert record


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


def test_workflow_simple_csv_item_metadata_iter_processing_success(
    simple_csv_workflow_instance, mocked_s3
):
    metadata_df = pd.DataFrame(
        {"filename": ["123.pdf", "456.pdf"], "TITLE": ["Cheeses of the World", np.nan]}
    )

    # upload to mocked S3 bucket
    csv_buffer = io.StringIO()
    metadata_df.to_csv(csv_buffer, index=False)
    mocked_s3.put_object(
        Bucket="dsc",
        Key="simple_csv/batch-aaa/metadata.csv",
        Body=csv_buffer.getvalue(),
    )

    metadata_iter = simple_csv_workflow_instance.item_metadata_iter(
        metadata_file="metadata.csv"
    )
    assert list(metadata_iter) == [
        {"item_identifier": "123.pdf", "title": "Cheeses of the World"},
        {"item_identifier": "456.pdf", "title": None},
    ]


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
