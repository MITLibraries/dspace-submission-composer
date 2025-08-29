import io
import json
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
from freezegun import freeze_time
from pynamodb.exceptions import DoesNotExist

from dsc.db.models import ItemSubmissionDB, ItemSubmissionStatus
from dsc.exceptions import ReconcileFailedMissingBitstreamsError
from dsc.item_submission import ItemSubmission


@freeze_time("2025-01-01 09:00:00")
@patch("dsc.utilities.aws.s3.S3Client.files_iter")
def test_workflow_simple_csv_reconcile_items_success(
    mock_s3_client_files_iter,
    mocked_item_submission_db,
    mocked_s3_simple_csv,
    caplog,
    simple_csv_workflow_instance,
):
    mock_s3_client_files_iter.return_value = [
        "s3://dsc/simple-csv/batch-aaa/123_001.pdf",
        "s3://dsc/simple-csv/batch-aaa/123_002.pdf",
    ]
    expected_reconcile_summary = {
        "reconciled": 1,
        "bitstreams_without_metadata": 0,
        "metadata_without_bitstreams": 0,
    }

    reconciled = simple_csv_workflow_instance.reconcile_items()
    item_submission_record = ItemSubmissionDB.get(hash_key="batch-aaa", range_key="123")

    assert reconciled
    assert item_submission_record.status == ItemSubmissionStatus.RECONCILE_SUCCESS
    assert json.dumps(expected_reconcile_summary) in caplog.text


@freeze_time("2025-01-01 09:00:00")
@patch("dsc.utilities.aws.s3.S3Client.files_iter")
def test_workflow_simple_csv_reconcile_items_if_not_reconciled_success(
    mock_s3_client_files_iter,
    mocked_item_submission_db,
    mocked_s3_simple_csv,
    caplog,
    simple_csv_workflow_instance,
):
    mock_s3_client_files_iter.return_value = [
        "s3://dsc/simple-csv/batch-aaa/123_001.pdf",
        "s3://dsc/simple-csv/batch-aaa/123_002.pdf",
        "s3://dsc/simple-csv/batch-aaa/456_003.pdf",
    ]
    expected_reconcile_summary = {
        "reconciled": 1,
        "bitstreams_without_metadata": 1,
        "metadata_without_bitstreams": 0,
    }

    reconciled = simple_csv_workflow_instance.reconcile_items()
    item_submission_record = ItemSubmissionDB.get(hash_key="batch-aaa", range_key="123")

    assert not reconciled
    assert item_submission_record.status == ItemSubmissionStatus.RECONCILE_SUCCESS
    assert json.dumps(expected_reconcile_summary) in caplog.text

    # since item identifiers are retrieved from SimpleCSV.item_metadata_iter
    # item identifiers associated with bitstreams without metadata are
    # NOT written to the DynamoDB table
    with pytest.raises(DoesNotExist):
        ItemSubmissionDB.get(hash_key="batch-aaa", range_key="456")


@patch("dsc.utilities.aws.s3.S3Client.files_iter")
def test_workflow_simple_csv_reconcile_item_success(
    mock_s3_client_files_iter,
    simple_csv_workflow_instance,
    item_metadata,
):
    mock_s3_client_files_iter.return_value = [
        "s3://dsc/simple-csv/batch-aaa/123_001.pdf",
        "s3://dsc/simple-csv/batch-aaa/123_002.pdf",
    ]

    # create item submission and attach source metadata
    item_submission = ItemSubmission.create(
        batch_id="aaa", item_identifier="123", workflow_name="simple-csv"
    )
    item_submission.source_metadata = item_metadata

    assert simple_csv_workflow_instance.reconcile_item(item_submission)


@patch("dsc.utilities.aws.s3.S3Client.files_iter")
def test_workflow_simple_csv_reconcile_item_if_no_bitstreams_success(
    mock_s3_client_files_iter,
    simple_csv_workflow_instance,
):
    mock_s3_client_files_iter.return_value = [
        "s3://dsc/simple-csv/batch-aaa/123_001.pdf",
        "s3://dsc/simple-csv/batch-aaa/123_002.pdf",
    ]

    # create item submission and attach source metadata
    item_submission = ItemSubmission.create(
        batch_id="aaa", item_identifier="124", workflow_name="simple-csv"
    )

    with pytest.raises(ReconcileFailedMissingBitstreamsError):
        simple_csv_workflow_instance.reconcile_item(item_submission)


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
        Key="simple-csv/batch-aaa/metadata.csv",
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
def test_workflow_simple_csv_get_item_bitstream_uris_if_prefix_id_success(
    mock_s3_client_files_iter, simple_csv_workflow_instance
):
    mock_s3_client_files_iter.return_value = [
        "s3://dsc/simple-csv/batch-aaa/123_001.pdf",
        "s3://dsc/simple-csv/batch-aaa/123_002.pdf",
    ]

    assert simple_csv_workflow_instance.get_item_bitstream_uris(
        item_identifier="123"
    ) == [
        "s3://dsc/simple-csv/batch-aaa/123_001.pdf",
        "s3://dsc/simple-csv/batch-aaa/123_002.pdf",
    ]


@patch("dsc.utilities.aws.s3.S3Client.files_iter")
def test_workflow_simple_csv_get_item_bitstream_uris_if_filename_id_success(
    mock_s3_client_files_iter, simple_csv_workflow_instance
):
    mock_s3_client_files_iter.return_value = [
        "s3://dsc/simple-csv/batch-aaa/123.pdf",
    ]

    assert simple_csv_workflow_instance.get_item_bitstream_uris(
        item_identifier="123.pdf"
    ) == ["s3://dsc/simple-csv/batch-aaa/123.pdf"]
