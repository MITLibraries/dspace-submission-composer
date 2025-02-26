# ruff: noqa: B015, SLF001
import json
from unittest.mock import patch

import pytest

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
    reconciled = opencourseware_workflow_instance.reconcile_bitstreams_and_metadata()

    assert reconciled
    assert (
        "Successfully reconciled bitstreams and metadata for all 1 item(s)"
    ) in caplog.text


@patch("dsc.workflows.opencourseware.OpenCourseWare._extract_metadata_from_zip_file")
@patch("dsc.utilities.aws.s3.S3Client.files_iter")
def test_workflow_ocw_reconcile_bitstreams_and_metadata_if_no_metadata_success(
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
    expected_reconcile_summary = {
        "reconciled": 1,
        "bitstreams_without_metadata": 1,
    }

    reconciled = opencourseware_workflow_instance.reconcile_bitstreams_and_metadata()

    assert not reconciled
    assert f"Reconcile results: {json.dumps(expected_reconcile_summary)}" in caplog.text
    assert "Failed to reconcile bitstreams and metadata" in caplog.text
    assert "Bitstreams without metadata: ['124']" in caplog.text


def test_workflow_ocw_extract_metadata_from_zip_file_success(
    mocked_s3,
    opencourseware_workflow_instance,
):
    """Performs metadata extraction from test zip file.

    The zip file (opencourseware/123.zip) represents a bitstream
    with metadata (includes a 'data.json' file).
    """
    with open("tests/fixtures/opencourseware/123.zip", "rb") as zip_file:
        mocked_s3.put_object(
            Bucket="dsc",
            Key="opencourseware/batch-aaa/123.zip",
            Body=zip_file,
        )

    assert opencourseware_workflow_instance._extract_metadata_from_zip_file(
        "opencourseware/batch-aaa/123.zip"
    ) == {
        "course_description": "Investigating the paranormal, one burger at a time.",
        "course_title": "Burgers and Beyond",
        "site_uid": "2318fd9f-1b5c-4a48-8a04-9c56d902a1f8",
        "instructors": ["Burger, Cheese E."],
        "topics": [
            "Fast Food - Handhelds - Burgers",
            "Paranormal - Perishable - Burgers",
        ],
    }


def test_workflow_ocw_extract_metadata_from_zip_file_without_metadata_raise_error(
    mocked_s3,
    opencourseware_workflow_instance,
):
    """Performs metadata extraction from test zip file.

    The zip file (opencourseware/124.zip) represents a bitstream
    with metadata (includes a 'data.json' file).
    """
    with open("tests/fixtures/opencourseware/124.zip", "rb") as zip_file:
        mocked_s3.put_object(
            Bucket="dsc",
            Key="opencourseware/batch-aaa/124.zip",
            Body=zip_file,
        )

    with pytest.raises(FileNotFoundError):
        opencourseware_workflow_instance._extract_metadata_from_zip_file(
            "opencourseware/batch-aaa/124.zip"
        )


def test_workflow_ocw_get_instructors_list_if_single_success(
    opencourseware_workflow_instance,
):
    assert opencourseware_workflow_instance._get_instructors_list(INSTRUCTORS[:1]) == [
        "Oki, Kerry"
    ]


def test_workflow_ocw_get_instructors_list_if_multiple_success(
    opencourseware_workflow_instance,
):
    assert opencourseware_workflow_instance._get_instructors_list(INSTRUCTORS) == [
        "Oki, Kerry",
        "Bird, Earl E.",
    ]


def test_workflow_ocw_get_instructors_list_if_any_names_empty_success(
    opencourseware_workflow_instance,
):
    instructors = INSTRUCTORS.copy()

    # the first four entries in the list below result in an empty name ("")
    # only the last entry is included
    instructors.extend(
        [
            {},  # all fields missing
            {"middle_initial": "E."},  # all required fields missing
            {"first_name": "Cheese", "middle_initial": "E."},  # "last_name" field missing
            {"last_name": "Burger", "middle_initial": "E."},  # "first_name" field missing
            {"first_name": "Cheese", "last_name": "Burger", "middle_initial": "E."},
        ]
    )
    assert opencourseware_workflow_instance._get_instructors_list(instructors) == [
        "Oki, Kerry",
        "Bird, Earl E.",
        "Burger, Cheese E.",
    ]


def test_workflow_ocw_get_instructors_list_if_all_names_empty_success(
    opencourseware_workflow_instance,
):
    assert opencourseware_workflow_instance._get_instructors_list([{}, {}]) == []
    assert opencourseware_workflow_instance._get_instructors_list([]) == []


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


def test_workflow_ocw_get_topics_list_success(opencourseware_workflow_instance):
    topics = [
        ["Fast Food", "Handhelds", "Burgers"],
        ["Paranormal", "Perishable", "Burgers"],
    ]
    assert opencourseware_workflow_instance._get_topics_list(topics) == [
        "Fast Food - Handhelds - Burgers",
        "Paranormal - Perishable - Burgers",
    ]


def test_workflow_ocw_get_topics_list_if_any_topics_empty_success(
    opencourseware_workflow_instance,
):
    topics = [["Fast Food", "Handhelds", "Burgers"], [""], []]
    assert opencourseware_workflow_instance._get_topics_list(topics) == [
        "Fast Food - Handhelds - Burgers"
    ]


def test_workflow_ocw_get_topics_list_if_all_topics_empty_success(
    opencourseware_workflow_instance,
):
    assert opencourseware_workflow_instance._get_topics_list([]) == []


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
