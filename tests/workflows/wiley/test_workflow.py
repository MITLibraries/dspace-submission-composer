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


@pytest.fixture
def mock_item_submission():
    """Factory for a fake ItemSubmission with sensible defaults."""

    def _make(item_identifier="001", message_id="abc", *, ready_to_submit=True):
        item = MagicMock(name=f"ItemSubmission({item_identifier})")
        item.item_identifier = item_identifier
        item.ready_to_submit.return_value = ready_to_submit
        item.prepare_dspace_metadata.return_value = None
        item.send_submission_message.return_value = {"MessageId": message_id}
        item.upsert_db.return_value = None
        return item

    return _make


@patch("dsc.workflows.wiley.workflow.Wiley._load_batch_manifest")
@patch("dsc.workflows.wiley.workflow.Wiley._get_transformed_metadata")
@patch("dsc.workflows.wiley.workflow.ItemSubmission.get_batch")
def test_workflow_submit_items_success(
    mock_item_submission_get_batch,
    mock_get_transformed_metadata,
    mock_load_batch_manifest,
    wiley_workflow_instance,
    mock_item_submission,
    caplog,
):
    mock_item_submission_get_batch.return_value = [
        mock_item_submission(
            item_identifier="10.1234/abcd",
            ready_to_submit=True,
            message_id="message-001",
        ),
        mock_item_submission(item_identifier="10.5678/efgh", ready_to_submit=False),
    ]
    mock_load_batch_manifest.return_value = {
        "10.1234/abcd": {
            "metadata_file": "s3://dsc/wiley/batch-aaa/10.1234-abcd/10.1234-abcd.json",
            "bitstream_files": [
                "s3://dsc/wiley/batch-aaa/10.1234-abcd/10.1234-abcd.pdf",
            ],
        }
    }
    mock_get_transformed_metadata.return_value = {"dc.title": ["Title"]}

    items = wiley_workflow_instance.submit_items()

    assert items == [{"item_identifier": "10.1234/abcd", "message_id": "message-001"}]
    assert (
        json.dumps({"total": 2, "created": 0, "skipped": 1, "errors": 0}) in caplog.text
    )
    mock_get_transformed_metadata.assert_called_once_with(
        source_metadata_file="s3://dsc/wiley/batch-aaa/10.1234-abcd/10.1234-abcd.json",
    )


@patch("dsc.workflows.wiley.workflow.Wiley._load_batch_manifest")
@patch("dsc.workflows.wiley.workflow.Wiley._get_transformed_metadata")
@patch("dsc.workflows.wiley.workflow.ItemSubmission.get_batch")
def test_workflow_submit_items_handles_errors(
    mock_item_submission_get_batch,
    mock_get_transformed_metadata,
    mock_load_batch_manifest,
    wiley_workflow_instance,
    mock_item_submission,
    caplog,
):
    mock_item_submission_get_batch.return_value = [
        mock_item_submission(item_identifier="10.1234/abcd", ready_to_submit=True),
        mock_item_submission(item_identifier="10.5678/efgh", ready_to_submit=False),
    ]
    mock_load_batch_manifest.return_value = {
        "10.1234/abcd": {
            "metadata_file": "s3://dsc/wiley/batch-aaa/10.1234-abcd/10.1234-abcd.json",
            "bitstream_files": [
                "s3://dsc/wiley/batch-aaa/10.1234-abcd/10.1234-abcd.pdf",
            ],
        }
    }
    mock_get_transformed_metadata.side_effect = Exception("boom")

    items = wiley_workflow_instance.submit_items()

    assert items == []
    assert (
        json.dumps({"total": 2, "created": 0, "skipped": 1, "errors": 1}) in caplog.text
    )


@patch("dsc.workflows.wiley.workflow.S3Client.files_iter")
def test_workflow_load_batch_manifest_success(mock_files_iter, wiley_workflow_instance):
    mock_files_iter.return_value = [
        "s3://dsc/wiley/batch-aaa/10.1234-abcd/10.1234-abcd.json",
        "s3://dsc/wiley/batch-aaa/10.1234-abcd/10.1234-abcd.pdf",
    ]

    assert wiley_workflow_instance._load_batch_manifest() == {
        "10.1234/abcd": {
            "metadata_file": "s3://dsc/wiley/batch-aaa/10.1234-abcd/10.1234-abcd.json",
            "bitstream_files": [
                "s3://dsc/wiley/batch-aaa/10.1234-abcd/10.1234-abcd.pdf",
            ],
        }
    }


@patch("dsc.workflows.wiley.workflow.WileyTransformer.transform")
def test_workflow_get_transformed_metadata_success(
    mock_transform,
    wiley_workflow_instance,
    tmp_path,
):
    source_metadata_file = tmp_path / "10.1234-abcd.json"
    source_metadata_file.write_text(json.dumps({"title": ["Title"]}))
    mock_transform.return_value = {"dc.title": ["Title"]}

    assert wiley_workflow_instance._get_transformed_metadata(
        source_metadata_file=str(source_metadata_file),
    ) == {"dc.title": ["Title"]}
    mock_transform.assert_called_once()


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
