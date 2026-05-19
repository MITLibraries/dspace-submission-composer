# ruff: noqa: SLF001
import glob
import json
import os
from unittest.mock import MagicMock, patch

import pytest
from dspace_rest_client.models import Item as DSpaceItem
from freezegun import freeze_time
from lxml import etree

from dsc import exceptions
from dsc.item_submission import ItemSubmission
from dsc.workflows.digitized_theses import (
    DigitizedTheses,
)

# =========================
# DSpace client and items
# =========================


@pytest.fixture
def mock_dspace_client():
    """A MagicMock standing in for an authenticated DSpaceClient."""
    client = MagicMock(name="DSpaceClient")
    client.authenticate.return_value = True
    return client


@pytest.fixture
def patched_dspace_client(mock_dspace_client):
    """Patch DSpaceClient.

    Patches DSpaceClient at the location it's imported in workflow.py.
    """
    with patch("dsc.workflows.digitized_theses.workflow.DSpaceClient") as dspace_client:
        dspace_client.return_value = mock_dspace_client
        yield dspace_client


@pytest.fixture
def dspace_item_digitized_thesis():
    """Item for a digitized (non-electronic) thesis.

    Content of an Item object returned by the search_objects() method
    of dspace_rest_client.client.DSpaceClient.
    """
    with open(
        "tests/fixtures/digitized-theses/dspace_item_digitized_thesis.json"
    ) as file:
        content = json.load(file)

    return DSpaceItem(content)


@pytest.fixture
def dspace_item_electronic_thesis():
    """Item for an electronic (student-submitted) thesis.

    Content of an Item object returned by the search_objects() method
    of dspace_rest_client.client.DSpaceClient.
    """
    with open(
        "tests/fixtures/digitized-theses/dspace_item_electronic_thesis.json"
    ) as file:
        content = json.load(file)

    return DSpaceItem(content)


# ====================
# Alma SRU responses
# ====================


@pytest.fixture
def alma_sru_response_single_record():
    with open(
        "tests/fixtures/digitized-theses/alma_sru_response_single_record.xml", "rb"
    ) as file:
        return file.read()


@pytest.fixture
def alma_sru_response_multiple_records():
    """Alma SRU response when multiple records are returned.

    The content for this fixture only highlights the tag that the workflow
    method for parsing a record relies on: {http://www.loc.gov/zing/srw/}numberOfRecords.
    """
    with open(
        "tests/fixtures/digitized-theses/alma_sru_response_multiple_records.xml", "rb"
    ) as file:
        return file.read()


@pytest.fixture
def alma_sru_response_no_record():
    with open(
        "tests/fixtures/digitized-theses/alma_sru_response_no_record.xml", "rb"
    ) as file:
        return file.read()


# ====================
# S3 bucket contents
# ====================


@pytest.fixture
def mock_s3_digitized_theses(mocked_s3, s3_client):
    for source_metadata_file in glob.glob(
        "tests/fixtures/digitized-theses/batch-aaa/*/*.xml"
    ):
        with open(source_metadata_file, "rb") as file:
            s3_client.put_file(
                bucket="dsc",
                key=source_metadata_file.replace("tests/fixtures/", ""),
                file_content=file.read(),
            )


def test_workflow_dspace_client_init(mock_dspace_client, patched_dspace_client):
    workflow = DigitizedTheses(batch_id="batch-aaa")
    _ = workflow.dspace_client

    mock_dspace_client.authenticate.assert_called_once()
    patched_dspace_client.assert_called_once_with(
        api_endpoint="mock://mit-dspace.test.4science.cloud/server/api",
        username="user@test.com",
        password="topsecret",  # noqa: S106
        fake_user_agent=True,
    )


def test_workflow_dspace_client_raise_authentication_error(
    mock_dspace_client, patched_dspace_client
):
    mock_dspace_client.authenticate.return_value = False
    workflow = DigitizedTheses(batch_id="batch-aaa")

    with pytest.raises(exceptions.DSpaceClientAuthenticationError):

        _ = workflow.dspace_client


@freeze_time("2025-01-01 09:00:00")
def test_workflow_update_batch_id():
    workflow = DigitizedTheses(batch_id="batch-aaa")

    assert workflow._update_batch_id(batch_id="batch-aaa") == "batch-aaa-20250101T090000Z"


@patch("dsc.workflows.digitized_theses.workflow.requests")
def test_workflow_download_metadata_from_alma(
    mock_requests, alma_sru_response_single_record, tmp_path
):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = alma_sru_response_single_record
    mock_requests.get.return_value = mock_response

    workflow = DigitizedTheses(batch_id="batch-aaa")
    workflow._download_metadata_from_alma(
        item_submission=ItemSubmission(
            batch_id="batch-aaa",
            item_identifier="36570527",
            workflow_name=workflow.workflow_name,
        ),
        batch_location=tmp_path,
    )

    assert os.path.exists(tmp_path / "36570527.xml")


def test_workflow_get_item_from_dspace(
    mock_dspace_client, patched_dspace_client, dspace_item_digitized_thesis
):
    mock_dspace_client.search_objects.return_value = [dspace_item_digitized_thesis]

    workflow = DigitizedTheses(batch_id="batch-aaa")
    result = workflow._get_item_from_dspace(item_identifier="12854433")

    assert result == dspace_item_digitized_thesis


def test_workflow_move_batch_files_to_theses_subfolders(tmp_path):
    """Verifies that theses are organized by `ItemSubmission.status_details`."""
    workflow = DigitizedTheses(batch_id="batch-aaa")
    workflow._move_batch_files_to_theses_subfolders(
        item_submissions=[
            ItemSubmission(
                batch_id="batch-aaa",
                item_identifier="001",
                workflow_name=workflow.workflow_name,
                status_details="Replacement thesis",
            ),
            ItemSubmission(
                batch_id="batch-aaa",
                item_identifier="002",
                workflow_name=workflow.workflow_name,
                status_details="New thesis",
            ),
            ItemSubmission(
                batch_id="batch-aaa",
                item_identifier="003",
                workflow_name=workflow.workflow_name,
                status_details="Error occurred",
            ),
        ],
        batch_location=tmp_path,
    )

    assert os.path.exists(tmp_path / "replacement-theses/001")
    assert os.path.exists(tmp_path / "new-theses/002")
    assert os.path.exists(tmp_path / "skipped-theses/003")


def test_workflow_is_replacement_thesis_if_digitized_returns_true(
    dspace_item_digitized_thesis,
):
    workflow = DigitizedTheses(batch_id="batch-aaa")

    assert workflow._is_replacement_thesis(dspace_item_digitized_thesis)


def test_workflow_is_replacement_thesis_if_electronic_returns_false(
    dspace_item_electronic_thesis,
):
    workflow = DigitizedTheses(batch_id="batch-aaa")

    assert not workflow._is_replacement_thesis(dspace_item_electronic_thesis)


def test_workflow_parse_record_from_sru_response_single_records(
    alma_sru_response_single_record,
):
    workflow = DigitizedTheses(batch_id="batch-aaa")

    record = workflow._parse_record_from_sru_response(alma_sru_response_single_record)

    assert isinstance(record, etree._Element)
    assert record.tag == "{http://www.loc.gov/MARC21/slim}record"


def test_workflow_parse_record_from_sru_response_multiple_records(
    alma_sru_response_multiple_records,
):
    workflow = DigitizedTheses(batch_id="batch-aaa")

    with pytest.raises(exceptions.ItemMetadataNotFoundError):
        workflow._parse_record_from_sru_response(alma_sru_response_multiple_records)


def test_workflow_parse_record_from_sru_response_no_records(alma_sru_response_no_record):
    workflow = DigitizedTheses(batch_id="batch-aaa")

    with pytest.raises(exceptions.ItemMetadataNotFoundError):
        workflow._parse_record_from_sru_response(alma_sru_response_no_record)


def test_workflow_parse_item_identifier():
    workflow = DigitizedTheses(batch_id="batch-aaa")

    assert workflow.parse_item_identifier("s3://bucket/prefix/123456-MIT.pdf") == "123456"
