# ruff: noqa: B015, SLF001
from unittest.mock import MagicMock, patch

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
    opencourseware_source_metadata,
    opencourseware_workflow_instance,
):
    mock_s3_client_files_iter.return_value = [
        "s3://dsc/opencourseware/batch-aaa/123.zip",
        "s3://dsc/opencourseware/batch-aaa/124.zip",
    ]
    opencourseware_source_metadata["instructors"] = "Edelman, Alan|Johnson, Steven G."
    mock_opencourseware_extract_metadata_from_zip_file.side_effect = [
        opencourseware_source_metadata,
        FileNotFoundError,
    ]
    assert opencourseware_workflow_instance.reconcile_bitstreams_and_metadata() == (
        set(),
        {"124"},
    )


def test_workflow_ocw_build_bitstream_dict_success(
    mocked_s3, opencourseware_workflow_instance, s3_client
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
    s3_client.put_file(
        file_content="",
        bucket="dsc",
        key="opencourseware/batch-aaa/ignore_me.txt",
    )
    assert opencourseware_workflow_instance._build_bitstream_dict() == {
        "123": ["opencourseware/batch-aaa/123.zip"],
        "124": ["opencourseware/batch-aaa/124.zip"],
    }


@patch("dsc.workflows.opencourseware.OpenCourseWare._extract_metadata_from_zip_file")
def test_workflow_ocw_identify_bitstreams_with_metadata_success(
    mock_opencourseware_extract_metadata_from_zip_file,
    opencourseware_source_metadata,
    opencourseware_workflow_instance,
):
    opencourseware_source_metadata["instructors"] = "Edelman, Alan|Johnson, Steven G."
    mock_opencourseware_extract_metadata_from_zip_file.side_effect = [
        opencourseware_source_metadata,
        FileNotFoundError,
    ]

    assert opencourseware_workflow_instance._identify_bitstreams_with_metadata(
        item_identifiers=["123", "124"]
    ) == ["123"]


@patch("dsc.workflows.opencourseware.OpenCourseWare._read_metadata_json_file")
@patch("zipfile.ZipFile")
@patch("smart_open.open")
def test_workflow_ocw_extract_metadata_from_zip_file_success(
    mock_smart_open,
    mock_zipfile,
    mock_opencourseware_read_metadata_json_file,
    opencourseware_source_metadata,
    opencourseware_workflow_instance,
):
    mock_file = MagicMock()
    mock_smart_open.return_value.__enter__.return_value = mock_file

    # create a mock for the infolist method to return a list of mock ZipInfo objects
    mock_zip = MagicMock()
    mock_zipfile.return_value.__enter__.return_value = mock_zip
    mock_zip.namelist.return_value = ["123/data.json", "123/file.txt"]

    opencourseware_source_metadata["instructors"] = "Edelman, Alan|Johnson, Steven G."
    mock_opencourseware_read_metadata_json_file.return_value = (
        opencourseware_source_metadata
    )

    assert opencourseware_workflow_instance._extract_metadata_from_zip_file(
        mock_file, "123"
    )


@patch("zipfile.ZipFile")
@patch("smart_open.open")
def test_workflow_ocw_extract_metadata_from_zip_file_without_metadata_raise_error(
    mock_smart_open, mock_zipfile, opencourseware_workflow_instance
):

    mock_file = MagicMock()
    mock_smart_open.return_value.__enter__.return_value = mock_file

    # create a mock for the infolist method to return a list of mock ZipInfo objects
    mock_zip = MagicMock()
    mock_zipfile.return_value.__enter__.return_value = mock_zip
    mock_zip.namelist.return_value = ["123/not_data.json", "123/file.txt"]

    with pytest.raises(FileNotFoundError):
        opencourseware_workflow_instance._extract_metadata_from_zip_file(mock_file, "123")


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
