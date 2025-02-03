# ruff: noqa: B015, SLF001
import json
from unittest.mock import patch

import pytest

from dsc.exceptions import ReconcileError

INSTRUCTORS = [
    {
        "first_name": "Kerry",
        "last_name": "Oki",
        "middle_initial": "",
        "salutation": "Prof.",
        "title": "Prof. Kerry Oki",
    },
    {
        "first_name": "Earl",
        "last_name": "Bird",
        "middle_initial": "E.",
        "salutation": "Prof.",
        "title": "Prof. Earl E. Bird",
    },
]


@patch("dsc.workflows.opencourseware.OpenCourseWare._extract_metadata_from_zip_file")
@patch("dsc.utilities.aws.s3.S3Client.files_iter")
def test_workflow_ocw_reconcile_bitstreams_and_metadata_success(
    mock_s3_client_files_iter,
    mock_opencourseware_extract_metadata_from_zip_file,
    caplog,
    opencourseware_source_metadata,
    opencourseware_workflow_instance,
):
    mock_s3_client_files_iter.return_value = ["s3://dsc/simple_csv/batch-aaa/123.zip"]
    mock_opencourseware_extract_metadata_from_zip_file.return_value = (
        opencourseware_source_metadata
    )
    opencourseware_workflow_instance.reconcile_bitstreams_and_metadata()
    assert (
        "Successfully reconciled bitstreams and metadata for all items (n=1)"
    ) in caplog.text


@patch("dsc.workflows.opencourseware.OpenCourseWare._extract_metadata_from_zip_file")
@patch("dsc.utilities.aws.s3.S3Client.files_iter")
def test_workflow_ocw_reconcile_bitstreams_and_metadata_if_no_metadata_raise_error(
    mock_s3_client_files_iter,
    mock_opencourseware_extract_metadata_from_zip_file,
    caplog,
    opencourseware_source_metadata,
    opencourseware_workflow_instance,
):
    opencourseware_source_metadata["instructors"] = "Edelman, Alan|Johnson, Steven G."
    mock_s3_client_files_iter.return_value = [
        "s3://dsc/opencourseware/batch-aaa/123.zip",
        "s3://dsc/opencourseware/batch-aaa/124.zip",
    ]
    mock_opencourseware_extract_metadata_from_zip_file.side_effect = [
        opencourseware_source_metadata,
        FileNotFoundError,
    ]
    expected_reconcile_error_message = {
        "note": "Failed to reconcile bitstreams and metadata.",
        "bitstreams_without_metadata": {
            "count": 1,
            "identifiers": ["124"],
        },
    }
    with pytest.raises(ReconcileError):
        opencourseware_workflow_instance.reconcile_bitstreams_and_metadata()

    assert json.dumps(expected_reconcile_error_message) in caplog.text


def test_workflow_ocw_extract_metadata_from_zip_file_success(
    opencourseware_workflow_instance,
):
    """Performs metadata extraction from test zip file.

    The zip file (opencourseware/123.zip) represents a bitstream
    with metadata (includes a 'data.json' file).
    """
    assert opencourseware_workflow_instance._extract_metadata_from_zip_file(
        "tests/fixtures/opencourseware/123.zip", "123"
    ) == {
        "course_description": "Investigating the paranormal, one burger at a time.",
        "course_title": "Burgers and Beyond",
        "instructors": "Burger, Cheese E.",
        "site_uid": "2318fd9f-1b5c-4a48-8a04-9c56d902a1f8",
    }


def test_workflow_ocw_extract_metadata_from_zip_file_without_metadata_raise_error(
    opencourseware_workflow_instance,
):
    """Performs metadata extraction from test zip file.

    The zip file (opencourseware/124.zip) represents a bitstream
    with metadata (includes a 'data.json' file).
    """
    with pytest.raises(FileNotFoundError):
        opencourseware_workflow_instance._extract_metadata_from_zip_file(
            "tests/fixtures/opencourseware/124.zip", "124"
        )


def test_workflow_ocw_get_instructors_delimited_string_if_single_success(
    opencourseware_workflow_instance,
):
    assert (
        opencourseware_workflow_instance._get_instructors_delimited_string(
            INSTRUCTORS[:1]
        )
        == "Oki, Kerry"
    )


def test_workflow_ocw_get_instructors_delimited_string_if_multiple_success(
    opencourseware_workflow_instance,
):
    assert (
        opencourseware_workflow_instance._get_instructors_delimited_string(INSTRUCTORS)
        == "Oki, Kerry|Bird, Earl E."
    )


def test_workflow_ocw_get_instructors_delimited_string_if_any_names_empty_success(
    opencourseware_workflow_instance,
):
    instructors = INSTRUCTORS.copy()

    # the first four entries in the list below result in an empty name ("")
    # only the last entry is included in the "|"-delimited string
    instructors.extend(
        [
            {},  # all fields missing
            {"middle_initial": "E."},  # all required fields missing
            {"first_name": "Cheese", "middle_initial": "E."},  # "last_name" field missing
            {"last_name": "Burger", "middle_initial": "E."},  # "first_name" field missing
            {"first_name": "Cheese", "last_name": "Burger", "middle_initial": "E."},
        ]
    )
    assert (
        opencourseware_workflow_instance._get_instructors_delimited_string(instructors)
        == "Oki, Kerry|Bird, Earl E.|Burger, Cheese E."
    )


def test_workflow_ocw_get_instructors_delimited_string_if_all_names_empty_success(
    opencourseware_workflow_instance,
):
    assert (
        opencourseware_workflow_instance._get_instructors_delimited_string([{}, {}]) == ""
    )
    assert opencourseware_workflow_instance._get_instructors_delimited_string([]) == ""


def test_workflow_ocw_construct_instructor_name_success(
    opencourseware_workflow_instance,
):
    instructor = {"first_name": "Kerry", "last_name": "Oki", "middle_initial": ""}
    opencourseware_workflow_instance._construct_instructor_name(
        instructor
    ) == "Oki, Kerry"


def test_workflow_ocw_construct_instructor_name_if_all_fields_blank_success(
    opencourseware_workflow_instance,
):
    instructor = {"first_name": "", "last_name": "", "middle_initial": ""}
    opencourseware_workflow_instance._construct_instructor_name(instructor) == ""


def test_workflow_ocw_construct_instructor_name_if_all_fields_missing_success(
    opencourseware_workflow_instance,
):
    opencourseware_workflow_instance._construct_instructor_name({}) == ""


def test_workflow_ocw_construct_instructor_name_if_required_fields_missing_success(
    opencourseware_workflow_instance,
):
    opencourseware_workflow_instance._construct_instructor_name(
        {"middle_initial": ""}
    ) == ""

    opencourseware_workflow_instance._construct_instructor_name(
        {"first_name": "Kerry", "middle_initial": ""}
    ) == ""

    opencourseware_workflow_instance._construct_instructor_name(
        {"last_name": "Oki", "middle_initial": ""}
    ) == ""


def test_workflow_ocw_get_item_identifier_success(
    opencourseware_workflow_instance, item_metadata
):
    assert opencourseware_workflow_instance.get_item_identifier(item_metadata) == "123"


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
