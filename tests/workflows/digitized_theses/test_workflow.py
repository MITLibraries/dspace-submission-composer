# ruff: noqa: SLF001
import glob
import json
import os
from collections import defaultdict
from unittest.mock import MagicMock, patch

import pytest
from dspace_rest_client.models import Item as DSpaceItem
from freezegun import freeze_time
from lxml import etree

from dsc import exceptions
from dsc.db.models import ItemSubmissionStatus
from dsc.item_submission import ItemSubmission
from dsc.workflows.digitized_theses import (
    DigitizedTheses,
)

# ===================================
# Fixtures: DSpace client and items
# ===================================


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


# ==============================
# Fixtures: Alma SRU responses
# ==============================


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


# ==============================
# Fixtures: S3 bucket contents
# ==============================


@pytest.fixture
def mock_s3_digitized_theses_dsc(mocked_s3, s3_client):
    """Mock batch for digitized theses in DSC S3 bucket."""
    for source_metadata_file in glob.glob(
        "tests/fixtures/digitized-theses/batch-aaa/**/*.xml", recursive=True
    ):
        with open(source_metadata_file, "rb") as file:
            s3_client.put_file(
                bucket="dsc",
                key=source_metadata_file.replace("tests/fixtures/", ""),
                file_content=file.read(),
            )


# ==================================
# Fixtures: ItemSubmission objects
# ==================================


@pytest.fixture
def mock_item_submission():
    """Factory for a fake ItemSubmission with sensible defaults."""

    def _make(item_identifier="001", message_id="abc", *, ready_to_submit=True):
        item = MagicMock(name=f"ItemSubmission({item_identifier})")
        item.item_identifier = item_identifier
        item.ready_to_submit.return_value = ready_to_submit
        item.send_submission_message.return_value = {"MessageId": message_id}
        return item

    return _make


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


@patch("dsc.workflows.digitized_theses.workflow.DigitizedTheses._get_item_from_dspace")
def test_workflow_get_item_submissions_from_synced_batch_replacement(
    mock_workflow_get_item_from_dspace, mock_s3_digitized_theses_dsc
):
    """Verify workflow can get item submissions from synced batch.

    This test uses mock_s3_digitized_theses, which represents a previously
    created batch in the DSC S3 bucket (i.e., contents organized into
    theses subfolders). This test shows the workflow's ability to
    generate ItemSubmissions based on the contents of the existing
    batch in the DSC S3 bucket.
    """
    mock_response = MagicMock()
    mock_response.handle = "1721.1/157651"
    mock_workflow_get_item_from_dspace.return_value = mock_response

    workflow = DigitizedTheses(batch_id="batch-aaa")
    results = workflow._get_item_submissions_from_synced_batch()

    assert results == [
        ItemSubmission(
            batch_id="batch-aaa",
            item_identifier="05588126",
            workflow_name="digitized-theses",
            dspace_handle="1721.1/157651",
            status=ItemSubmissionStatus.CREATE_SUCCESS,
            status_details="Replacement thesis",
        )
    ]


@patch("dsc.workflows.digitized_theses.workflow.DigitizedTheses._get_item_from_dspace")
def test_workflow_get_item_submissions_from_synced_batch_replacement_not_found(
    mock_workflow_get_item_from_dspace,
    mock_s3_digitized_theses_dsc,
):
    mock_workflow_get_item_from_dspace.side_effect = exceptions.DSpaceClientSearchError(
        "Error occurred"
    )
    workflow = DigitizedTheses(batch_id="batch-aaa")
    results = workflow._get_item_submissions_from_synced_batch()

    assert results == [
        ItemSubmission(
            batch_id="batch-aaa",
            item_identifier="05588126",
            workflow_name="digitized-theses",
            dspace_handle=None,
            status=ItemSubmissionStatus.CREATE_SKIPPED,
            status_details="Error occurred",
        )
    ]


@patch("dsc.workflows.digitized_theses.workflow.S3Client.files_iter")
def test_workflow_get_item_submissions_from_synced_batch_new(mock_s3client_files_iter):
    mock_s3client_files_iter.return_value = [
        "tests/fixtures/digitized-theses/batch-aaa/new-theses/05588126/05588126.xml"
    ]
    workflow = DigitizedTheses(batch_id="batch-aaa")
    results = workflow._get_item_submissions_from_synced_batch()

    assert results == [
        ItemSubmission(
            batch_id="batch-aaa",
            item_identifier="05588126",
            workflow_name="digitized-theses",
            dspace_handle=None,
            status=ItemSubmissionStatus.CREATE_SUCCESS,
            status_details="New thesis",
        )
    ]


@patch("dsc.workflows.digitized_theses.workflow.S3Client.files_iter")
def test_workflow_get_item_submissions_from_synced_batch_skipped(
    mock_s3client_files_iter,
):
    mock_s3client_files_iter.return_value = [
        "tests/fixtures/digitized-theses/batch-aaa/skipped-theses/05588126/05588126.xml"
    ]
    workflow = DigitizedTheses(batch_id="batch-aaa")
    results = workflow._get_item_submissions_from_synced_batch()

    assert results == [
        ItemSubmission(
            batch_id="batch-aaa",
            item_identifier="05588126",
            workflow_name="digitized-theses",
            dspace_handle=None,
            status=ItemSubmissionStatus.CREATE_SKIPPED,
            status_details="Skipped thesis",
        )
    ]


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


@patch(
    "dsc.workflows.digitized_theses.workflow.DigitizedTheses._get_item_collection_handle"
)
@patch("dsc.workflows.digitized_theses.workflow.DigitizedTheses.get_item_bitstream_uris")
@patch(
    "dsc.workflows.digitized_theses.workflow.DigitizedTheses._get_transformed_metadata"
)
@patch("dsc.workflows.digitized_theses.workflow.DigitizedTheses._load_batch_manifest")
@patch("dsc.workflows.digitized_theses.workflow.ItemSubmission.upsert_db")
@patch("dsc.workflows.digitized_theses.workflow.ItemSubmission.prepare_dspace_metadata")
@patch("dsc.workflows.digitized_theses.workflow.ItemSubmission.get_batch")
def test_workflow_submit_items_success(
    mock_item_submission_get_batch,
    mock_item_submission_prepare_dspace_metadata,
    mock_item_submission_upsert_db,
    mock_workflow_load_batch_manifest,
    mock_workflow_get_transformed_metadata,
    mock_workflow_get_item_bitstream_uris,
    mock_workflow_get_item_collection_handle,
    mock_item_submission,
    caplog,
):
    """Test control flow of DigitizedTheses.submit_items.

    This tests the scenario in which a batch comprises of two item submissions:
    one ready to submit and one that is not. This assumes a happy path in which
    all sub methods run without error.
    """
    # mock ItemSubmission methods
    mock_item_submission_get_batch.return_value = [
        mock_item_submission(
            item_identifier="001", ready_to_submit=True, message_id="message-001"
        ),
        mock_item_submission(item_identifier="002", ready_to_submit=False),
    ]
    mock_item_submission_prepare_dspace_metadata.return_value = None
    mock_item_submission_upsert_db.return_value = None

    # mock workflow methods
    mock_workflow_load_batch_manifest.return_value = {
        "001": {
            "thesis_type": "New thesis",
            "metadata_file": ["001.xml"],
            "bitstream_files": ["001.pdf"],
        }
    }
    mock_workflow_get_transformed_metadata.return_value = None
    mock_workflow_get_item_bitstream_uris.return_value = None
    mock_workflow_get_item_collection_handle.return_value = None

    workflow = DigitizedTheses(batch_id="batch-aaa")
    workflow.submit_items()

    assert (
        json.dumps({"total": 2, "submitted": 1, "skipped": 1, "errors": 0}) in caplog.text
    )


@patch(
    "dsc.workflows.digitized_theses.workflow.DigitizedTheses._get_item_collection_handle"
)
@patch("dsc.workflows.digitized_theses.workflow.DigitizedTheses.get_item_bitstream_uris")
@patch(
    "dsc.workflows.digitized_theses.workflow.DigitizedTheses._get_transformed_metadata"
)
@patch("dsc.workflows.digitized_theses.workflow.DigitizedTheses._load_batch_manifest")
@patch("dsc.workflows.digitized_theses.workflow.ItemSubmission.upsert_db")
@patch("dsc.workflows.digitized_theses.workflow.ItemSubmission.prepare_dspace_metadata")
@patch("dsc.workflows.digitized_theses.workflow.ItemSubmission.get_batch")
def test_workflow_submit_items_handles_errors(
    mock_item_submission_get_batch,
    mock_item_submission_prepare_dspace_metadata,
    mock_item_submission_upsert_db,
    mock_workflow_load_batch_manifest,
    mock_workflow_get_transformed_metadata,
    mock_workflow_get_item_bitstream_uris,
    mock_workflow_get_item_collection_handle,
    mock_item_submission,
    caplog,
):
    """Test control flow of DigitizedTheses.submit_items.

    This tests the scenario in which a batch comprises of two item submissions:
    one ready to submit and one that is not. The test throws
    exceptions.ItemMetadataNotFoundError when DSC calls _get_item_metadata() for
    the item submission ready for submission. The test demonstrates that if any
    exception is raised in the try-except block, all errors--except for
    NotImplementedError--are handled and simply recorded.
    """
    # mock ItemSubmission methods
    mock_item_submission_get_batch.return_value = [
        mock_item_submission(
            item_identifier="001", ready_to_submit=True, message_id="message-001"
        ),
        mock_item_submission(item_identifier="002", ready_to_submit=False),
    ]
    mock_item_submission_prepare_dspace_metadata.return_value = None
    mock_item_submission_upsert_db.return_value = None

    # mock workflow methods
    mock_workflow_load_batch_manifest.return_value = {
        "001": {
            "thesis_type": "New thesis",
            "metadata_file": ["001.xml"],
            "bitstream_files": ["001.pdf"],
        }
    }
    mock_workflow_get_transformed_metadata.side_effect = Exception
    mock_workflow_get_item_bitstream_uris.return_value = None
    mock_workflow_get_item_collection_handle.return_value = None

    workflow = DigitizedTheses(batch_id="batch-aaa")
    workflow.submit_items()

    assert (
        json.dumps({"total": 2, "submitted": 0, "skipped": 1, "errors": 1}) in caplog.text
    )


def test_workflow_load_batch_manifest(mock_s3_digitized_theses_dsc):
    workflow = DigitizedTheses(batch_id="batch-aaa")
    assert workflow._load_batch_manifest() == defaultdict(
        dict,
        {
            "05588126": {
                "thesis_type": "Replacement thesis",
                "metadata_file": "s3://dsc/digitized-theses/batch-aaa/replacement-theses/05588126/05588126.xml",
            }
        },
    )


@freeze_time("2025-01-01 09:00:00")
def test_workflow_get_transformed_metadata(mock_s3_digitized_theses_dsc):
    workflow = DigitizedTheses(batch_id="batch-aaa")
    item_metadata = workflow._get_transformed_metadata(
        item_identifier="05588126",
        source_metadata_file="tests/fixtures/digitized-theses/batch-aaa/replacement-theses/05588126/05588126.xml",
    )

    assert item_metadata["dc.identifier.oclc"] == "05588126"
    assert item_metadata["dc.title"] == [
        "Global solvability of invariant differential operators."
    ]
    assert item_metadata["dspace.imported"] == "2025-01-01T09:00:00Z"
    assert "2025-01-01T09:00:00Z" in " | ".join(
        item_metadata["dc.description.provenance"]
    )


def test_workflow_get_item_collection_handle():
    workflow = DigitizedTheses(batch_id="batch-aaa")

    assert (
        workflow._get_item_collection_handle(
            item_metadata={"mit.thesis.degree": ["Bachelor"]}
        )
        == "1721.1/131024"
    )


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

    assert workflow.parse_item_identifier("s3://bucket/prefix/123456.pdf") == "123456"
