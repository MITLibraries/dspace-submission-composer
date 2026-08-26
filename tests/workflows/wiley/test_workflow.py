# ruff: noqa: SLF001
import json
from unittest.mock import MagicMock, patch

import pytest
from freezegun import freeze_time

from dsc import exceptions
from dsc.db.models import ItemSubmissionStatus
from dsc.item_submission import ItemSubmission
from dsc.workflows.wiley.workflow import Wiley


@pytest.fixture(autouse=True)
def _test_env_wiley(monkeypatch):
    monkeypatch.setenv("WILEY_BITSTREAM_API_URL", "mock.com/doi/am-pdf/")
    monkeypatch.setenv("WILEY_METADATA_API_URL", "mock.com/works/")


@pytest.fixture
def wiley_workflow_instance():
    return Wiley(batch_id="batch-aaa")


@patch("dsc.workflows.wiley.workflow.Wiley._download_bitstream")
@patch("dsc.workflows.wiley.workflow.Wiley._get_crossref_metadata")
def test_workflow_prepare_item_submission_success(
    mock_get_crossref_metadata,
    mock_download_bitstream,
    wiley_workflow_instance,
    tmp_path,
):
    result = wiley_workflow_instance._prepare_item_submission(
        doi="10.1234/abcd",
        output_dir=str(tmp_path),
    )

    assert result == ItemSubmission(
        batch_id="batch-aaa",
        item_identifier="10.1234/abcd",
        workflow_name="wiley",
        status=ItemSubmissionStatus.CREATE_SUCCESS,
    )
    mock_download_bitstream.assert_called_once_with(
        item_identifier="10.1234/abcd",
        output_dir=str(tmp_path),
    )
    mock_get_crossref_metadata.assert_called_once_with(
        item_identifier="10.1234/abcd",
        output_dir=str(tmp_path),
    )


@patch("dsc.workflows.wiley.workflow.Wiley._download_bitstream")
def test_workflow_prepare_item_submission_failed(
    mock_download_bitstream,
    wiley_workflow_instance,
    tmp_path,
):
    mock_download_bitstream.side_effect = exceptions.ItemBitstreamsNotFoundError

    result = wiley_workflow_instance._prepare_item_submission(
        doi="10.1234/abcd",
        output_dir=str(tmp_path),
    )

    assert result == ItemSubmission(
        batch_id="batch-aaa",
        item_identifier="10.1234/abcd",
        workflow_name="wiley",
        status=ItemSubmissionStatus.CREATE_FAILED,
        status_details="No bitstreams found for the item submission",
    )


@patch("dsc.workflows.wiley.workflow.requests.get")
def test_workflow_download_bitstream_success(
    mock_requests_get, wiley_workflow_instance, tmp_path
):
    mock_response = MagicMock()
    mock_response.headers = {"content-type": "application/pdf"}
    mock_response.content = b"pdf-bytes"
    mock_response.raise_for_status.return_value = None
    mock_requests_get.return_value = mock_response

    wiley_workflow_instance._download_bitstream(
        item_identifier="10.1234/abcd",
        output_dir=str(tmp_path),
    )

    pdf_path = tmp_path / "10.1234-abcd" / "10.1234-abcd.pdf"
    assert pdf_path.exists()
    assert pdf_path.read_bytes() == b"pdf-bytes"


@patch("dsc.workflows.wiley.workflow.requests.get")
def test_workflow_get_crossref_metadata_success(
    mock_requests_get, wiley_workflow_instance, tmp_path
):
    mock_response = MagicMock()
    mock_response.json.return_value = {"message": {"title": ["Title"]}}
    mock_requests_get.return_value = mock_response

    wiley_workflow_instance._get_crossref_metadata(
        item_identifier="10.1234/abcd",
        output_dir=str(tmp_path),
    )

    metadata_path = tmp_path / "10.1234-abcd" / "10.1234-abcd.json"
    assert metadata_path.exists()
    assert json.loads(metadata_path.read_text()) == {"message": {"title": ["Title"]}}


@freeze_time("2025-01-01 09:00:00")
def test_workflow_update_batch_id():
    workflow = Wiley(batch_id="batch-aaa")

    assert workflow._update_batch_id(batch_id="batch-aaa") == "batch-aaa-20250101T090000Z"
