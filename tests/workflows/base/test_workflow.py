import json
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from freezegun import freeze_time

from dsc import exceptions
from dsc.db.models import ItemSubmissionDB, ItemSubmissionStatus
from dsc.item_submission import ItemSubmission
from dsc.workflows.base import Workflow

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


def test_workflow_get_workflow_success():
    # get workflow by name
    workflow_class = Workflow.get_workflow(workflow_name="test")
    workflow_instance = workflow_class(batch_id="batch-aaa")

    assert workflow_instance.workflow_name == "test"
    assert workflow_instance.submission_system == "Test@MIT"
    assert workflow_instance.batch_id == "batch-aaa"
    assert workflow_instance.s3_bucket == "dsc"
    assert workflow_instance.output_queue == "mock-output-queue"


def test_workflow_get_workflow_invalid_workflow_name_raises_error():
    with pytest.raises(exceptions.InvalidWorkflowNameError):
        Workflow.get_workflow("does-not-exist")


@patch("dsc.workflows.base.workflow.CONFIG")
def test_workflow_check_required_env_vars_success(
    mock_config, monkeypatch, test_workflow_instance
):
    monkeypatch.setattr(
        type(test_workflow_instance),
        "required_env_vars",
        ["TEST_METADATA_API_URL"],
        raising=False,
    )
    mock_config.test_metadata_api_url = "cool-url"
    test_workflow_instance.required_env_vars = ["TEST_METADATA_API_URL"]
    test_workflow_instance.check_required_env_vars()


def test_workflow_check_required_env_vars_raises_error(
    monkeypatch, test_workflow_instance, caplog
):
    monkeypatch.setattr(
        type(test_workflow_instance),
        "required_env_vars",
        ["TEST_METADATA_API_URL"],
        raising=False,
    )
    with pytest.raises(RuntimeError):
        test_workflow_instance.check_required_env_vars()


@freeze_time("2025-01-01 09:00:00")
def test_workflow_create_batch_in_db_success(
    mock_item_submission_db,
    test_workflow_instance,
):
    test_workflow_instance._create_batch_in_db(  # noqa: SLF001
        item_submissions=[
            ItemSubmission(
                batch_id="batch-aaa",
                item_identifier="123",
                workflow_name="test",
                status=ItemSubmissionStatus.CREATE_SUCCESS,
            ),
            ItemSubmission(
                batch_id="batch-aaa",
                item_identifier="789",
                workflow_name="test",
                status=ItemSubmissionStatus.CREATE_SUCCESS,
            ),
        ],
    )
    item_submission = ItemSubmissionDB.get(hash_key="batch-aaa", range_key="123")
    assert item_submission.last_run_date == datetime(2025, 1, 1, 9, 0, tzinfo=UTC)
    assert item_submission.status == ItemSubmissionStatus.CREATE_SUCCESS


@patch("dsc.workflows.base.workflow.Workflow._publish_count_metric")
@patch("dsc.workflows.base.workflow.Workflow.get_batch_bitstream_uris")
@patch("dsc.workflows.base.workflow.Workflow.item_metadata_iter")
@patch("dsc.workflows.base.workflow.ItemSubmission.prepare_dspace_metadata")
@patch("dsc.workflows.base.workflow.ItemSubmission.upsert_db")
@patch("dsc.workflows.base.workflow.ItemSubmission.get_batch")
def test_workflow_submit_items_success(
    mock_item_submission_get_batch,
    mock_item_submission_upsert_db,
    mock_item_submission_prepare_dspace_metadata,
    mock_workflow_item_metadata_iter,
    mock_workflow_get_batch_bitstream_uris,
    mock_workflow_publish_count_metric,
    mock_item_submission,
    test_workflow_instance,
    caplog,
):
    """Test control flow of base Workflow.submit_items() method.

    This tests the scenario in which a batch comprises of two item submissions:
    one ready to submit and one that is not. This assumes a happy path in which
    all sub methods run without error.
    """
    # mock ItemSubmission methods
    mock_item_submission_get_batch.return_value = [
        mock_item_submission(
            item_identifier="123", ready_to_submit=True, message_id="message-001"
        ),
        mock_item_submission(item_identifier="789", ready_to_submit=False),
    ]
    mock_item_submission_prepare_dspace_metadata.return_value = None
    mock_item_submission_upsert_db.return_value = None

    # mock Workflow methods
    mock_workflow_item_metadata_iter.return_value = [
        {
            "dc.title": "Title",
            "dc.contributor": "Author 1|Author 2",
            "item_identifier": "123",
        },
        {
            "dc.title": "2nd Title",
            "dc.contributor": "Author 3|Author 4",
            "item_identifier": "789",
        },
    ]
    mock_workflow_get_batch_bitstream_uris.return_value = [
        "s3://dsc/test/batch-aaa/123_01.pdf",
        "s3://dsc/test/batch-aaa/123_02.pdf",
        "s3://dsc/test/batch-aaa/789_01.pdf",
    ]

    test_workflow_instance.submit_items(collection_handle="12345/5678")

    assert (
        json.dumps({"total": 2, "submitted": 1, "skipped": 1, "errors": 0}) in caplog.text
    )


@patch("dsc.workflows.base.workflow.Workflow._publish_count_metric")
@patch("dsc.workflows.base.workflow.Workflow._run_metadata_transformer")
@patch("dsc.workflows.base.workflow.Workflow.get_batch_bitstream_uris")
@patch("dsc.workflows.base.workflow.Workflow.item_metadata_iter")
@patch("dsc.workflows.base.workflow.ItemSubmission.get_batch")
def test_workflow_submit_items_handles_metadata_transformation_errors(
    mock_item_submission_get_batch,
    mock_workflow_item_metadata_iter,
    mock_workflow_get_batch_bitstream_uris,
    mock_workflow_run_metadata_transformer,
    mock_workflow_publish_count_metric,
    mock_item_submission,
    test_workflow_instance,
    caplog,
):
    """Test control flow of base Workflow.submit_items() method.

    This tests the scenario in which a batch comprises of two item submissions:
    one ready to submit and one that is not. The test throws
    exceptions.MetadataTransformationError when transforming source metadata
    for the item submission. The test demonstrates that if any
    exception is raised in the try-except block, all errors--except for
    NotImplementedError--are handled and simply recorded.
    """
    # mock ItemSubmission methods
    mock_item_submission_get_batch.return_value = [
        mock_item_submission(
            item_identifier="123", ready_to_submit=True, message_id="message-001"
        ),
        mock_item_submission(item_identifier="789", ready_to_submit=False),
    ]

    # mock Workflow methods
    mock_workflow_item_metadata_iter.return_value = [
        {
            "dc.title": "Title",
            "dc.contributor": "Author 1|Author 2",
            "item_identifier": "123",
        },
        {
            "dc.title": "2nd Title",
            "dc.contributor": "Author 3|Author 4",
            "item_identifier": "789",
        },
    ]
    mock_workflow_get_batch_bitstream_uris.return_value = [
        "s3://dsc/test/batch-aaa/123_01.pdf",
        "s3://dsc/test/batch-aaa/123_02.pdf",
        "s3://dsc/test/batch-aaa/789_01.pdf",
    ]
    mock_workflow_run_metadata_transformer.side_effect = (
        exceptions.MetadataTransformationError
    )

    with patch.object(test_workflow_instance, "metadata_transformer", MagicMock()):
        test_workflow_instance.submit_items(collection_handle="12345/5678")

    assert (
        json.dumps({"total": 2, "submitted": 0, "skipped": 1, "errors": 1}) in caplog.text
    )


@patch("dsc.workflows.base.workflow.Workflow._publish_count_metric")
@patch("dsc.workflows.base.workflow.Workflow.get_batch_bitstream_uris")
@patch("dsc.workflows.base.workflow.Workflow.item_metadata_iter")
@patch("dsc.workflows.base.workflow.ItemSubmission.get_batch")
def test_workflow_submit_items_no_collection_handle_raises_error(
    mock_item_submission_get_batch,
    mock_workflow_item_metadata_iter,
    mock_workflow_get_batch_bitstream_uris,
    mock_workflow_publish_count_metric,
    mock_item_submission,
    test_workflow_instance,
    caplog,
):
    # mock ItemSubmission methods
    mock_item_submission_get_batch.return_value = [
        mock_item_submission(
            item_identifier="123", ready_to_submit=True, message_id="message-001"
        ),
        mock_item_submission(item_identifier="789", ready_to_submit=False),
    ]

    # mock Workflow methods
    mock_workflow_item_metadata_iter.return_value = [
        {
            "dc.title": "Title",
            "dc.contributor": "Author 1|Author 2",
            "item_identifier": "123",
        },
        {
            "dc.title": "2nd Title",
            "dc.contributor": "Author 3|Author 4",
            "item_identifier": "789",
        },
    ]
    mock_workflow_get_batch_bitstream_uris.return_value = [
        "s3://dsc/test/batch-aaa/123_01.pdf",
        "s3://dsc/test/batch-aaa/123_02.pdf",
        "s3://dsc/test/batch-aaa/789_01.pdf",
    ]

    with pytest.raises(NotImplementedError):
        test_workflow_instance.submit_items()


def _make_result_message(message_id, attributes, body):
    return {
        "MessageId": message_id,
        "ReceiptHandle": f"receipt-{message_id}",
        "MessageAttributes": attributes,
        "Body": body,
    }


@patch("dsc.workflows.base.workflow.Workflow._publish_count_metric")
@patch("dsc.workflows.base.workflow.SQSClient")
@patch("dsc.workflows.base.workflow.ItemSubmission.get_batch")
def test_workflow_finalize_items_success(
    mock_item_submission_get_batch,
    mock_sqs_client_class,
    mock_workflow_publish_count_metric,
    mock_item_submission,
    test_workflow_instance,
    result_message_attributes,
    result_message_body_success,
    result_message_body_error,
    caplog,
):
    """Test control flow of base Workflow.finalize_items() method.

    This tests the scenario in which a batch comprises of two item submissions,
    one that ingested successfully and one that failed to ingest, matched against
    corresponding result messages received from the DSS output queue.
    """
    caplog.set_level("DEBUG")

    # mock ItemSubmission methods
    # create ItemSubmission post 'submit' step, representing successful ingest
    item_success = mock_item_submission(item_identifier="10.1002/term.3131")
    item_success.status = ItemSubmissionStatus.SUBMIT_SUCCESS
    item_success.ingest_attempts = 0

    # create ItemSubmission post 'submit' step, representing failed ingest
    item_failed = mock_item_submission(item_identifier="10.1002/term.4242")
    item_failed.status = ItemSubmissionStatus.SUBMIT_SUCCESS
    item_failed.ingest_attempts = 0
    mock_item_submission_get_batch.return_value = [item_success, item_failed]

    # mock result messages received by SQSClient
    error_attributes = {
        **result_message_attributes,
        "PackageID": {"DataType": "String", "StringValue": "10.1002/term.4242"},
    }
    mock_sqs_client = mock_sqs_client_class.return_value
    mock_sqs_client.receive.return_value = [
        _make_result_message(
            "message-001", result_message_attributes, result_message_body_success
        ),
        _make_result_message("message-002", error_attributes, result_message_body_error),
    ]

    test_workflow_instance.finalize_items()

    expected_processing_summary = {
        "received_messages": 2,
        "ingest_success": 1,
        "ingest_failed": 1,
        "ingest_unknown": 0,
    }

    assert (
        "Record with primary keys batch_id=batch-aaa (hash key) and item_identifier="
        "10.1002/term.3131 (range key) was ingested" in caplog.text
    )
    assert item_success.status == ItemSubmissionStatus.INGEST_SUCCESS
    assert item_success.ingest_attempts == 1

    assert (
        "Record with primary keys batch_id=batch-aaa (hash key) and "
        "item_identifier=10.1002/term.4242 (range key) failed to ingest" in caplog.text
    )
    assert item_failed.status == ItemSubmissionStatus.INGEST_FAILED
    assert item_failed.ingest_attempts == 1
    assert json.dumps(expected_processing_summary) in caplog.text


@patch("dsc.workflows.base.workflow.Workflow._publish_count_metric")
@patch("dsc.workflows.base.workflow.SQSClient")
@patch("dsc.workflows.base.workflow.ItemSubmission.get_batch")
def test_workflow_finalize_items_missing_result_message_skipped(
    mock_item_submission_get_batch,
    mock_sqs_client_class,
    mock_workflow_publish_count_metric,
    mock_item_submission,
    test_workflow_instance,
    result_message_attributes,
    result_message_body_success,
    caplog,
):
    caplog.set_level("DEBUG")

    # mock ItemSubmission methods
    # create ItemSubmission post 'submit' step, representing successful ingest
    item_with_result = mock_item_submission(item_identifier="10.1002/term.3131")
    item_with_result.status = ItemSubmissionStatus.SUBMIT_SUCCESS

    # create ItemSubmission post 'submit' step, representing an item whose status
    # cannot be determined (no corresponding result message)
    item_without_result = mock_item_submission(item_identifier="10.1002/term.4242")
    item_without_result.status = ItemSubmissionStatus.SUBMIT_SUCCESS
    mock_item_submission_get_batch.return_value = [
        item_with_result,
        item_without_result,
    ]

    # mock result messages received by SQSClient
    mock_sqs_client = mock_sqs_client_class.return_value
    mock_sqs_client.receive.return_value = [
        _make_result_message(
            "message-001", result_message_attributes, result_message_body_success
        ),
    ]

    test_workflow_instance.finalize_items()

    expected_processing_summary = {
        "received_messages": 1,
        "ingest_success": 1,
        "ingest_failed": 0,
        "ingest_unknown": 0,
    }
    assert json.dumps(expected_processing_summary) in caplog.text

    assert item_with_result.status == ItemSubmissionStatus.INGEST_SUCCESS
    item_with_result.upsert_db.assert_called_once()

    # item without a matching result message is skipped, status is unchanged
    assert item_without_result.status == ItemSubmissionStatus.SUBMIT_SUCCESS
    item_without_result.upsert_db.assert_not_called()


@patch("dsc.workflows.base.workflow.Workflow._publish_count_metric")
@patch("dsc.workflows.base.workflow.SQSClient")
@patch("dsc.workflows.base.workflow.ItemSubmission.get_batch")
def test_workflow_finalize_items_with_unknown_ingest_result(
    mock_item_submission_get_batch,
    mock_sqs_client_class,
    mock_workflow_publish_count_metric,
    mock_item_submission,
    test_workflow_instance,
    result_message_attributes,
    result_message_body_error,
    caplog,
):
    caplog.set_level("DEBUG")

    # mock ItemSubmission methods
    # create ItemSubmission post 'submit' step, representing an item whose status
    # cannot be determined (content of result message inconclusive)
    item_unknown = mock_item_submission(item_identifier="10.1002/term.4242")
    item_unknown.status = ItemSubmissionStatus.SUBMIT_SUCCESS
    item_unknown.ingest_attempts = 0
    mock_item_submission_get_batch.return_value = [item_unknown]

    # mock SQS output queue messages
    unknown_attributes = {
        **result_message_attributes,
        "PackageID": {"DataType": "String", "StringValue": "10.1002/term.4242"},
    }
    unknown_body = json.loads(result_message_body_error)
    unknown_body["ResultType"] = "false"
    mock_sqs_client = mock_sqs_client_class.return_value
    mock_sqs_client.receive.return_value = [
        _make_result_message("message-001", unknown_attributes, json.dumps(unknown_body)),
    ]

    test_workflow_instance.finalize_items()

    assert item_unknown.status == ItemSubmissionStatus.INGEST_UNKNOWN
    assert item_unknown.ingest_attempts == 1

    expected_summary = {
        "received_messages": 1,
        "ingest_success": 0,
        "ingest_failed": 0,
        "ingest_unknown": 1,
    }
    assert json.dumps(expected_summary) in caplog.text


@patch("dsc.workflows.base.workflow.Workflow._publish_count_metric")
@patch("dsc.workflows.base.workflow.SQSClient")
@patch("dsc.workflows.base.workflow.ItemSubmission.get_batch")
def test_workflow_finalize_items_exception_handled_and_logged(
    mock_item_submission_get_batch,
    mock_sqs_client_class,
    mock_workflow_publish_count_metric,
    test_workflow_instance,
    result_message_attributes,
    caplog,
):
    caplog.set_level("DEBUG")

    # no item submissions in the batch; the malformed message is enough to reproduce
    mock_item_submission_get_batch.return_value = []

    # mock SQS output queue messages
    mock_sqs_client = mock_sqs_client_class.return_value
    mock_sqs_client.receive.return_value = [
        _make_result_message(
            "message-001", result_message_attributes, '{"fail": "fail"}'
        ),
    ]

    test_workflow_instance.finalize_items()

    expected_summary = {
        "received_messages": 0,
        "ingest_success": 0,
        "ingest_failed": 0,
        "ingest_unknown": 0,
    }
    assert "Failure parsing message" in caplog.text
    assert json.dumps(expected_summary) in caplog.text


def test_workflow_workflow_specific_processing_success(test_workflow_instance, caplog):
    test_workflow_instance.workflow_specific_processing()
    assert "No extra processing for batch based on workflow: 'test'" in caplog.text


@patch("dsc.workflows.base.workflow.SESClient")
@patch("dsc.workflows.base.workflow.FinalizeReport.load")
def test_workflow_send_report_success(
    mock_finalize_report_load,
    mock_ses_client_class,
    test_workflow_instance,
    caplog,
):
    caplog.set_level("DEBUG")

    # mock Report methods
    mock_report = MagicMock()
    mock_report.subject = "Finalize report"
    mock_report.generate_summary.return_value = "Summary"
    mock_report.prepare_attachments.return_value = []
    mock_finalize_report_load.return_value = mock_report

    test_workflow_instance.send_report(
        step="finalize",
        email_recipients=["test@test.test"],
    )

    mock_report.upload_attachments.assert_called_once()
    mock_ses_client_class.return_value.create_and_send_email.assert_called_once()
    assert "Sent report to recipients: ['test@test.test']" in caplog.text
