# ruff: noqa: SLF001
from unittest.mock import patch

import pytest

from dsc.item_submission import ItemSubmission


@patch(
    "dsc.workflows.opencourseware.workflow.OpenCourseWare._read_metadata_from_zip_file"
)
def test_workflow_ocw_prepare_batch_success(
    mock_opencourseware_read_metadata_from_zip_file,
    mocked_item_submission_db,
    mocked_s3,
    opencourseware_itemsubmission_source_metadata,
    opencourseware_itemsubmission_dspace_metadata,
    opencourseware_workflow_instance,
    s3_client,
):
    s3_client.put_file(
        file_content="",
        bucket="dsc",
        key="opencourseware/batch-aaa/123.zip",
    )
    mock_opencourseware_read_metadata_from_zip_file.return_value = (
        opencourseware_itemsubmission_source_metadata
    )

    assert opencourseware_workflow_instance._prepare_batch() == (
        [
            ItemSubmission(
                batch_id="batch-aaa",
                item_identifier="123",
                workflow_name="opencourseware",
                dspace_metadata=opencourseware_itemsubmission_dspace_metadata,
            )
        ],
        [],
    )


@patch(
    "dsc.workflows.opencourseware.workflow.OpenCourseWare._read_metadata_from_zip_file"
)
def test_workflow_ocw_itemsubmission_create_dspace_metadataentry_success(
    mock_opencourseware_read_metadata_from_zip_file,
    caplog,
    mocked_s3,
    opencourseware_itemsubmission_source_metadata,
    opencourseware_itemsubmission_dspace_metadata,
    s3_client,
):
    s3_client.put_file(
        file_content="",
        bucket="dsc",
        key="opencourseware/batch-aaa/123.zip",
    )
    mock_opencourseware_read_metadata_from_zip_file.return_value = (
        opencourseware_itemsubmission_source_metadata
    )

    item_submission = ItemSubmission(
        batch_id="aaa",
        item_identifier="123",
        workflow_name="opencourseware",
        dspace_metadata=opencourseware_itemsubmission_dspace_metadata,
    )

    assert item_submission._create_dspace_metadataentry() == {
        "metadata": [
            {
                "key": "dc.title",
                "value": "14.02 Principles of Macroeconomics, Fall 2004",
            },
            {"key": "dc.date.issued", "value": "2004"},
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
            },
            {
                "key": "dc.contributor.author",
                "value": "Caballero, Ricardo",
            },
            {
                "key": "dc.contributor.department",
                "value": "Massachusetts Institute of Technology. Department of Economics",
            },
            {
                "key": "creativework.learningresourcetype",
                "value": "Problem Sets with Solutions",
            },
            {
                "key": "creativework.learningresourcetype",
                "value": "Exams with Solutions",
            },
            {
                "key": "creativework.learningresourcetype",
                "value": "Lecture Notes",
            },
            {
                "key": "dc.subject",
                "value": "Social Science - Economics - International Economics",
            },
            {
                "key": "dc.subject",
                "value": "Social Science - Economics - Macroeconomics",
            },
            {"key": "dc.identifier.other", "value": "14.02"},
            {"key": "dc.identifier.other", "value": "14.02-Fall2004"},
            {"key": "dc.coverage.temporal", "value": "Fall 2004"},
            {
                "key": "dc.audience.educationlevel",
                "value": "Undergraduate",
            },
            {"key": "dc.type", "value": "Learning Object"},
            {
                "key": "dc.rights",
                "value": "Attribution-NonCommercial-NoDerivs 4.0 United States",
            },
            {
                "key": "dc.rights.uri",
                "value": "https://creativecommons.org/licenses/by-nc-nd/4.0/deed.en",
            },
            {"key": "dc.language.iso", "value": "en_US"},
        ]
    }


@patch(
    "dsc.workflows.opencourseware.workflow.OpenCourseWare._read_metadata_from_zip_file"
)
def test_workflow_ocw_item_metadata_iter_success(
    mock_opencourseware_read_metadata_from_zip_file,
    caplog,
    mocked_s3,
    opencourseware_itemsubmission_source_metadata,
    opencourseware_workflow_instance,
    s3_client,
):
    s3_client.put_file(
        file_content="",
        bucket="dsc",
        key="opencourseware/batch-aaa/123.zip",
    )
    mock_opencourseware_read_metadata_from_zip_file.return_value = (
        opencourseware_itemsubmission_source_metadata
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
    opencourseware_itemsubmission_source_metadata,
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
        == opencourseware_itemsubmission_source_metadata
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
