# ruff: noqa: SLF001
import json
from unittest.mock import patch, PropertyMock

import pytest

from dsc.db.models import ItemSubmissionDB, ItemSubmissionStatus


@patch("dsc.workflows.opencourseware.OpenCourseWare._read_metadata_from_zip_file")
def test_workflow_ocw_reconcile_items_success(
    mock_opencourseware_read_metadata_from_zip_file,
    mocked_item_submission_db,
    caplog,
    opencourseware_workflow_instance,
    opencourseware_source_metadata,
):
    mock_opencourseware_read_metadata_from_zip_file.return_value = (
        opencourseware_source_metadata
    )
    expected_summary = {
        "reconciled": 1,
        "bitstreams_without_metadata": 0,
        "metadata_without_bitstreams": 0,
    }

    reconciled = opencourseware_workflow_instance.reconcile_items()
    item_submission_record = ItemSubmissionDB.get(hash_key="batch-aaa", range_key="123")

    assert reconciled
    assert item_submission_record.status == ItemSubmissionStatus.RECONCILE_SUCCESS
    assert json.dumps(expected_summary) in caplog.text


@patch("dsc.workflows.opencourseware.OpenCourseWare._read_metadata_from_zip_file")
@patch("dsc.item_submission.ItemSubmission.bitstream_s3_uris", new_callable=PropertyMock)
def test_workflow_ocw_reconcile_items_if_item_submission_exists_success(
    mock_item_submission_bitstream_s3_uris,
    mock_opencourseware_read_metadata_from_zip_file,
    mocked_item_submission_db,
    caplog,
    opencourseware_workflow_instance,
    opencourseware_source_metadata,
):
    mock_opencourseware_read_metadata_from_zip_file.return_value = (
        opencourseware_source_metadata
    )
    mock_item_submission_bitstream_s3_uris.return_value = [
        "s3://dsc/simple_csv/batch-aaa/123_001.pdf",
        "s3://dsc/simple_csv/batch-aaa/123_002.pdf",
    ]
    expected_summary = {
        "reconciled": 1,
        "bitstreams_without_metadata": 0,
        "metadata_without_bitstreams": 0,
    }

    # create record to force raise dsc.db.exceptions ItemSubmissionExistsError
    ItemSubmissionDB.create(
        batch_id="batch-aaa",
        item_identifier="123",
        workflow_name=opencourseware_workflow_instance.workflow_name,
        status=ItemSubmissionStatus.RECONCILE_SUCCESS,
    )
    item_submission_record = ItemSubmissionDB.get(hash_key="batch-aaa", range_key="123")

    reconciled = opencourseware_workflow_instance.reconcile_items()

    assert reconciled
    assert item_submission_record.status == ItemSubmissionStatus.RECONCILE_SUCCESS
    assert json.dumps(expected_summary) in caplog.text


@patch("dsc.workflows.opencourseware.OpenCourseWare._read_metadata_from_zip_file")
def test_workflow_ocw_reconcile_items_if_no_metadata_include_in_db(
    mock_opencourseware_read_metadata_from_zip_file,
    mocked_item_submission_db,
    caplog,
    opencourseware_workflow_instance,
    opencourseware_source_metadata,
):
    # override values in self.batch_bitstreams
    opencourseware_workflow_instance.batch_bitstreams = [
        "s3://dsc/opencourseware/batch-aaa/123.zip",
        "s3://dsc/opencourseware/batch-aaa/124.zip",
    ]
    mock_opencourseware_read_metadata_from_zip_file.side_effect = [
        opencourseware_source_metadata,
        FileNotFoundError,
    ]
    expected_summary = {
        "reconciled": 1,
        "bitstreams_without_metadata": 1,
        "metadata_without_bitstreams": 0,
    }

    reconciled = opencourseware_workflow_instance.reconcile_items()

    assert not reconciled
    assert json.dumps(expected_summary) in caplog.text

    # since item identifiers are retrieved from the bitstream filename
    # bitstreams without metadata
    # ARE written to the DynamoDB table
    assert ItemSubmissionDB.get(hash_key="batch-aaa", range_key="124")


@patch("dsc.workflows.opencourseware.OpenCourseWare._read_metadata_from_zip_file")
def test_workflow_ocw_item_metadata_iter_success(
    mock_opencourseware_read_metadata_from_zip_file,
    opencourseware_source_metadata,
    opencourseware_workflow_instance,
):
    mock_opencourseware_read_metadata_from_zip_file.return_value = (
        opencourseware_source_metadata
    )
    assert next(opencourseware_workflow_instance.item_metadata_iter()) == {
        "item_identifier": "123",
        "dc.title": "14.02 Principles of Macroeconomics, Fall 2004",
        "dc.date.issued": "2004",
        "dc.description.abstract": (
            "This course provides an overview of the following macroeconomic issues: "
            "the determination of output, employment, unemployment, interest rates, "
            "and inflation. Monetary and fiscal policies are discussed, as are public "
            "debt and international economic issues. This course also introduces basic "
            "models of macroeconomics and illustrates principles with the experience of "
            "the United States and other economies.\n"
        ),
        "dc.contributor.author": ["Caballero, Ricardo"],
        "dc.contributor.department": [
            "Massachusetts Institute of Technology. Department of Economics"
        ],
        "creativework.learningresourcetype": [
            "Problem Sets with Solutions",
            "Exams with Solutions",
            "Lecture Notes",
        ],
        "dc.subject": [
            "Social Science - Economics - International Economics",
            "Social Science - Economics - Macroeconomics",
        ],
        "dc.identifier.other": ["14.02", "14.02-Fall2004"],
        "dc.coverage.temporal": "Fall 2004",
        "dc.audience.educationlevel": "Undergraduate",
        "dc.type": "Learning Object",
        "dc.rights": ("Attribution-NonCommercial-NoDerivs 4.0 United States"),
        "dc.rights.uri": ("https://creativecommons.org/licenses/by-nc-nd/4.0/deed.en"),
        "dc.language.iso": "en_US",
    }


def test_workflow_ocw_read_metadata_from_zip_file_success(
    mocked_s3,
    opencourseware_source_metadata,
    opencourseware_workflow_instance,
):
    """Read source metadata JSON file from test zip file.

    The zip file (opencourseware/123.zip) represents a bitstream
    with metadata (includes a 'data.json' file).
    """
    with open("tests/fixtures/opencourseware/123.zip", "rb") as zip_file:
        mocked_s3.put_object(
            Bucket="dsc",
            Key="opencourseware/batch-aaa/123.zip",
            Body=zip_file,
        )

    assert (
        opencourseware_workflow_instance._read_metadata_from_zip_file(
            "s3://dsc/opencourseware/batch-aaa/123.zip"
        )
        == opencourseware_source_metadata
    )


def test_workflow_ocw_read_metadata_from_zip_file_without_metadata_raise_error(
    mocked_s3,
    opencourseware_workflow_instance,
):
    """Read source metadata JSON file from test zip file.

    The zip file (opencourseware/124.zip) represents a bitstream
    without metadata (does not include a 'data.json' file).
    """
    with open("tests/fixtures/opencourseware/124.zip", "rb") as zip_file:
        mocked_s3.put_object(
            Bucket="dsc",
            Key="opencourseware/batch-aaa/124.zip",
            Body=zip_file,
        )

    with pytest.raises(FileNotFoundError):
        opencourseware_workflow_instance._read_metadata_from_zip_file(
            "opencourseware/batch-aaa/124.zip"
        )


@patch("dsc.utilities.aws.s3.S3Client.files_iter")
def test_workflow_ocw_get_bitstreams_uris_success(
    mock_s3_client_files_iter, opencourseware_workflow_instance
):
    mock_s3_client_files_iter.return_value = [
        "s3://dsc/opencourseware/batch-aaa/123.zip",
        "s3://dsc/opencourseware/batch-aaa/124.zip",
    ]

    assert opencourseware_workflow_instance.get_bitstream_s3_uris(
        item_identifier="123.pdf"
    )
