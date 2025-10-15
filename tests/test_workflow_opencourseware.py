# ruff: noqa: SLF001
import json
from unittest.mock import patch

import pytest

from dsc.db.models import ItemSubmissionDB, ItemSubmissionStatus
from dsc.exceptions import ReconcileFailedMissingMetadataError
from dsc.item_submission import ItemSubmission


@patch("dsc.workflows.opencourseware.OpenCourseWare._read_metadata_from_zip_file")
def test_workflow_ocw_metadata_mapping_dspace_metadata_success(
    mock_opencourseware_read_metadata_from_zip_file,
    caplog,
    mocked_s3,
    opencourseware_source_metadata,
    opencourseware_workflow_instance,
    s3_client,
):
    s3_client.put_file(
        file_content="",
        bucket="dsc",
        key="opencourseware/batch-aaa/123.zip",
    )
    mock_opencourseware_read_metadata_from_zip_file.return_value = (
        opencourseware_source_metadata
    )

    item_submission = ItemSubmission(
        batch_id="aaa", item_identifier="123", workflow_name="opencourseware"
    )
    item_submission.create_dspace_metadata(
        item_metadata=next(opencourseware_workflow_instance.item_metadata_iter()),
        metadata_mapping=opencourseware_workflow_instance.metadata_mapping,
    )

    assert item_submission.dspace_metadata["metadata"] == [
        {
            "key": "dc.title",
            "value": "14.02 Principles of Macroeconomics, Fall 2004",
            "language": None,
        },
        {"key": "dc.date.issued", "value": "2004", "language": None},
        {
            "key": "dc.description.abstract",
            "value": (
                "This course provides an overview of the following macroeconomic "
                "issues: the determination of output, employment, unemployment, "
                "interest rates, and inflation. Monetary and fiscal policies are "
                "discussed, as are public debt and international economic issues. "
                "This course also introduces basic models of macroeconomics and "
                "illustrates principles with the experience of the United States "
                "and other economies.\n"
            ),
            "language": None,
        },
        {"key": "dc.contributor.author", "value": "Caballero, Ricardo", "language": None},
        {
            "key": "dc_contributor_department",
            "value": "Massachusetts Institute of Technology. Department of Economics",
            "language": None,
        },
        {
            "key": "creativework.learningresourcetype",
            "value": "Problem Sets with Solutions",
            "language": None,
        },
        {
            "key": "creativework.learningresourcetype",
            "value": "Exams with Solutions",
            "language": None,
        },
        {
            "key": "creativework.learningresourcetype",
            "value": "Lecture Notes",
            "language": None,
        },
        {
            "key": "dc.subject",
            "value": "Social Science - Economics - International Economics",
            "language": None,
        },
        {
            "key": "dc.subject",
            "value": "Social Science - Economics - Macroeconomics",
            "language": None,
        },
        {"key": "dc.identifier.other", "value": "14.02", "language": None},
        {"key": "dc.identifier.other", "value": "14.02-Fall2004", "language": None},
        {"key": "dc.coverage.temporal", "value": "Fall 2004", "language": None},
        {"key": "dc.audience.educationlevel", "value": "Undergraduate", "language": None},
        {"key": "dc.type", "value": "Learning Object", "language": None},
        {
            "key": "dc.rights",
            "value": "Attribution-NonCommercial-NoDerivs 4.0 United States",
            "language": None,
        },
        {
            "key": "dc.rights.uri",
            "value": "https://creativecommons.org/licenses/by-nc-nd/4.0/deed.en",
            "language": None,
        },
        {"key": "dc.language.iso", "value": "en_US", "language": None},
    ]


@patch("dsc.workflows.opencourseware.OpenCourseWare._read_metadata_from_zip_file")
def test_workflow_ocw_prepare_batch_success(
    mock_opencourseware_read_metadata_from_zip_file,
    mocked_item_submission_db,
    mocked_s3,
    opencourseware_source_metadata,
    opencourseware_workflow_instance,
    s3_client,
):
    s3_client.put_file(
        file_content="",
        bucket="dsc",
        key="opencourseware/batch-aaa/123.zip",
    )
    mock_opencourseware_read_metadata_from_zip_file.return_value = (
        opencourseware_source_metadata
    )

    assert opencourseware_workflow_instance.prepare_batch() == (
        [
            {
                "batch_id": "batch-aaa",
                "item_identifier": "123",
                "workflow_name": "opencourseware",
            }
        ],
        [],
    )


@patch("dsc.workflows.opencourseware.OpenCourseWare._read_metadata_from_zip_file")
def test_workflow_ocw_reconcile_items_success(
    mock_opencourseware_read_metadata_from_zip_file,
    mocked_item_submission_db,
    caplog,
    mocked_s3,
    opencourseware_source_metadata,
    opencourseware_workflow_instance,
    s3_client,
):
    s3_client.put_file(
        file_content="",
        bucket="dsc",
        key="opencourseware/batch-aaa/123.zip",
    )
    ItemSubmissionDB(
        item_identifier="123",
        batch_id="batch-aaa",
        workflow_name="opencourseware",
    ).create()

    mock_opencourseware_read_metadata_from_zip_file.return_value = (
        opencourseware_source_metadata
    )
    expected_reconcile_summary = {
        "reconciled": 1,
        "bitstreams_without_metadata": 0,
        "metadata_without_bitstreams": 0,
    }

    reconciled = opencourseware_workflow_instance.reconcile_items()
    item_submission_record = ItemSubmissionDB.get(hash_key="batch-aaa", range_key="123")

    assert reconciled
    assert item_submission_record.status == ItemSubmissionStatus.RECONCILE_SUCCESS
    assert json.dumps(expected_reconcile_summary) in caplog.text


@patch("dsc.workflows.opencourseware.OpenCourseWare._read_metadata_from_zip_file")
def test_workflow_ocw_reconcile_items_if_not_reconciled_success(
    mock_opencourseware_read_metadata_from_zip_file,
    mocked_item_submission_db,
    caplog,
    mocked_s3,
    opencourseware_source_metadata,
    opencourseware_workflow_instance,
    s3_client,
):
    s3_client.put_file(
        file_content="",
        bucket="dsc",
        key="opencourseware/batch-aaa/123.zip",
    )
    s3_client.put_file(
        file_content="",
        bucket="dsc",
        key="opencourseware/batch-aaa/124.zip",
    )
    ItemSubmissionDB(
        item_identifier="123",
        batch_id="batch-aaa",
        workflow_name="opencourseware",
    ).create()
    ItemSubmissionDB(
        item_identifier="124",
        batch_id="batch-aaa",
        workflow_name="opencourseware",
    ).create()

    mock_opencourseware_read_metadata_from_zip_file.side_effect = [
        opencourseware_source_metadata,
        FileNotFoundError,
    ]
    expected_reconcile_summary = {
        "reconciled": 1,
        "bitstreams_without_metadata": 1,
        "metadata_without_bitstreams": 0,
    }

    reconciled = opencourseware_workflow_instance.reconcile_items()
    item_submission_record_success = ItemSubmissionDB.get(
        hash_key="batch-aaa", range_key="123"
    )
    item_submission_record_failed = ItemSubmissionDB.get(
        hash_key="batch-aaa", range_key="124"
    )

    # since item identifiers are retrieved from the bitstream filename
    # bitstreams without metadata
    # ARE written to the DynamoDB table
    assert not reconciled
    assert item_submission_record_success.status == ItemSubmissionStatus.RECONCILE_SUCCESS
    assert item_submission_record_failed.status == ItemSubmissionStatus.RECONCILE_FAILED
    assert item_submission_record_failed.status_details == str(
        ReconcileFailedMissingMetadataError()
    )
    assert json.dumps(expected_reconcile_summary) in caplog.text


@patch("dsc.workflows.opencourseware.OpenCourseWare._read_metadata_from_zip_file")
def test_workflow_ocw_reconcile_item_success(
    mock_opencourseware_read_metadata_from_zip_file,
    mocked_s3,
    opencourseware_source_metadata,
    opencourseware_workflow_instance,
    s3_client,
):
    s3_client.put_file(
        file_content="",
        bucket="dsc",
        key="opencourseware/batch-aaa/123.zip",
    )
    mock_opencourseware_read_metadata_from_zip_file.side_effect = [
        opencourseware_source_metadata,
        FileNotFoundError,
    ]

    # create item submission and attach source metadata
    item_submission = ItemSubmission.create(
        batch_id="aaa", item_identifier="123", workflow_name="opencourseware"
    )
    item_submission.source_metadata = opencourseware_source_metadata

    assert opencourseware_workflow_instance.reconcile_item(item_submission)


@patch("dsc.workflows.opencourseware.OpenCourseWare._read_metadata_from_zip_file")
def test_workflow_ocw_reconcile_item_if_no_metadata_success(
    mock_opencourseware_read_metadata_from_zip_file,
    mocked_s3,
    opencourseware_source_metadata,
    opencourseware_workflow_instance,
    s3_client,
):
    s3_client.put_file(
        file_content="",
        bucket="dsc",
        key="opencourseware/batch-aaa/124.zip",
    )
    mock_opencourseware_read_metadata_from_zip_file.side_effect = [
        opencourseware_source_metadata,
        FileNotFoundError,
    ]

    # create item submission without attaching source metadata
    item_submission = ItemSubmission.create(
        batch_id="aaa", item_identifier="124", workflow_name="opencourseware"
    )

    with pytest.raises(ReconcileFailedMissingMetadataError):
        opencourseware_workflow_instance.reconcile_item(item_submission)


@patch("dsc.workflows.opencourseware.OpenCourseWare._read_metadata_from_zip_file")
def test_workflow_ocw_item_metadata_iter_success(
    mock_opencourseware_read_metadata_from_zip_file,
    caplog,
    mocked_s3,
    opencourseware_source_metadata,
    opencourseware_workflow_instance,
    s3_client,
):
    s3_client.put_file(
        file_content="",
        bucket="dsc",
        key="opencourseware/batch-aaa/123.zip",
    )
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
        "dc.audience.educationlevel": ["Undergraduate"],
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


def test_workflow_ocw_get_item_bitstream_uris_success(
    mocked_s3,
    opencourseware_workflow_instance,
    s3_client,
):
    s3_client.put_file(
        file_content="",
        bucket="dsc",
        key="opencourseware/batch-aaa/123.zip",
    )
    s3_client.put_file(
        file_content="",
        bucket="dsc",
        key="opencourseware/batch-aaa/123.zip",
    )

    assert opencourseware_workflow_instance.get_item_bitstream_uris(
        item_identifier="123.zip"
    )
