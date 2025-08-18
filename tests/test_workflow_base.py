import json
from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError

from dsc.db.models import ItemSubmissionDB, ItemSubmissionStatus
from dsc.exceptions import (
    InvalidWorkflowNameError,
)
from dsc.reports import FinalizeReport
from dsc.workflows.base import Workflow


def test_base_workflow_init_with_defaults_success():
    workflow_class = Workflow.get_workflow(workflow_name="test")
    workflow_instance = workflow_class(batch_id="batch-aaa")
    assert workflow_instance.workflow_name == "test"
    assert workflow_instance.submission_system == "Test@MIT"
    assert (
        workflow_instance.metadata_mapping_path
        == "tests/fixtures/test_metadata_mapping.json"
    )
    assert workflow_instance.batch_id == "batch-aaa"
    assert workflow_instance.s3_bucket == "dsc"
    assert workflow_instance.output_queue == "mock-output-queue"


def test_base_workflow_get_workflow_success():
    workflow_class = Workflow.get_workflow("test")
    assert workflow_class.workflow_name == "test"


def test_base_workflow_get_workflow_invalid_workflow_name_raises_error(
    base_workflow_instance,
):
    with pytest.raises(InvalidWorkflowNameError):
        base_workflow_instance.get_workflow("tast")


def test_base_workflow_reconcile_items_if_non_reconcile_raises_error(
    base_workflow_instance,
):
    with pytest.raises(
        TypeError,
        match=(
            "Method 'reconcile_items' not used by workflow 'TestWorkflow'"  # noqa: E501
        ),
    ):
        base_workflow_instance.reconcile_items()


def test_base_workflow_submit_items_success(
    caplog,
    base_workflow_instance,
    s3_client,
    mocked_s3,
    mocked_sqs_input,
    mocked_sqs_output,
    mocked_item_submission_db,
):
    caplog.set_level("DEBUG")
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/123_01.pdf")
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/123_02.jpg")
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/789_01.pdf")
    ItemSubmissionDB.create(
        item_identifier="123",
        batch_id="batch-aaa",
        workflow_name="test",
        status=ItemSubmissionStatus.RECONCILE_SUCCESS,
    )
    ItemSubmissionDB.create(
        item_identifier="789",
        batch_id="batch-aaa",
        workflow_name="test",
        status=ItemSubmissionStatus.RECONCILE_SUCCESS,
    )
    items = base_workflow_instance.submit_items(collection_handle="123.4/5678")

    expected_submission_summary = {"total": 2, "submitted": 2, "skipped": 0, "errors": 0}

    assert len(items) == 2  # noqa: PLR2004
    assert json.dumps(expected_submission_summary) in caplog.text


def test_base_workflow_submit_items_failed_ready_to_submit_is_skipped(
    caplog,
    base_workflow_instance,
    mocked_s3,
    mocked_sqs_input,
    mocked_sqs_output,
    mocked_item_submission_db,
):
    caplog.set_level("DEBUG")
    ItemSubmissionDB.create(
        item_identifier="123",
        batch_id="batch-aaa",
        workflow_name="test",
        status=ItemSubmissionStatus.INGEST_SUCCESS,
    )
    ItemSubmissionDB.create(
        item_identifier="789",
        batch_id="batch-aaa",
        workflow_name="test",
        status=ItemSubmissionStatus.RECONCILE_SUCCESS,
    )
    items = base_workflow_instance.submit_items(collection_handle="123.4/5678")

    expected_submission_summary = {"total": 2, "submitted": 1, "skipped": 1, "errors": 0}
    assert len(items) == 1
    assert (
        "Record with primary keys batch_id=batch-aaa (hash key) and "
        "item_identifier=123 (range key) already ingested, skipping submission"
        in caplog.text
    )
    assert json.dumps(expected_submission_summary) in caplog.text


@patch("dsc.item_submission.ItemSubmission.send_submission_message")
def test_base_workflow_submit_items_exceptions_handled(
    mocked_method,
    caplog,
    base_workflow_instance,
    mocked_s3,
    mocked_sqs_input,
    mocked_sqs_output,
    mocked_item_submission_db,
):
    side_effect = [
        {"MessageId": "abcd", "ResponseMetadata": {"HTTPStatusCode": 200}},
        ClientError(
            {
                "Error": {
                    "Code": "InvalidParameterValue",
                    "Message": "The specified S3 bucket does not exist.",
                }
            },
            "SendMessage",
        ),
    ]
    mocked_method.side_effect = side_effect
    ItemSubmissionDB.create(
        item_identifier="123",
        batch_id="batch-aaa",
        workflow_name="test",
        status=ItemSubmissionStatus.RECONCILE_SUCCESS,
    )
    ItemSubmissionDB.create(
        item_identifier="789",
        batch_id="batch-aaa",
        workflow_name="test",
        status=ItemSubmissionStatus.RECONCILE_SUCCESS,
    )
    items = base_workflow_instance.submit_items(collection_handle="123.4/5678")

    expected_submission_summary = {"total": 2, "submitted": 1, "skipped": 0, "errors": 1}

    assert len(items) == 1
    assert items == [{"item_identifier": "123", "message_id": "abcd"}]
    assert json.dumps(expected_submission_summary) in caplog.text


def test_base_workflow_finalize_items_success(
    caplog,
    base_workflow_instance,
    item_submission_instance,
    mocked_item_submission_db,
    mocked_sqs_output,
    result_message_attributes,
    result_message_body_success,
    result_message_body_error,
    sqs_client,
):
    caplog.set_level("DEBUG")

    ItemSubmissionDB.create(
        item_identifier="10.1002/term.3131",
        batch_id="batch-aaa",
        workflow_name="test",
        status=ItemSubmissionStatus.SUBMIT_SUCCESS,
    )
    ItemSubmissionDB.create(
        item_identifier="10.1002/term.4242",
        batch_id="batch-aaa",
        workflow_name="test",
        status=ItemSubmissionStatus.SUBMIT_SUCCESS,
    )

    sqs_client.send(
        message_attributes=result_message_attributes,
        message_body=result_message_body_success,
    )

    # create error result message
    result_message_attributes["PackageID"]["StringValue"] = "10.1002/term.4242"
    sqs_client.send(
        message_attributes=result_message_attributes,
        message_body=result_message_body_error,
    )

    expected_processing_summary = {
        "received_messages": 2,
        "ingest_success": 1,
        "ingest_failed": 1,
        "ingest_unknown": 0,
    }

    base_workflow_instance.finalize_items()

    assert (
        "Record with primary keys batch_id=batch-aaa (hash key) and item_identifier="
        "10.1002/term.3131 (range key) was ingested" in caplog.text
    )
    record_1 = ItemSubmissionDB.get("batch-aaa", "10.1002/term.3131")
    assert record_1.status == ItemSubmissionStatus.INGEST_SUCCESS
    assert record_1.ingest_attempts == 1

    assert (
        "Record with primary keys batch_id=batch-aaa (hash key) and "
        "item_identifier=10.1002/term.4242 (range key) failed to ingest" in caplog.text
    )
    record_2 = ItemSubmissionDB.get("batch-aaa", "10.1002/term.4242")
    assert record_2.status == ItemSubmissionStatus.INGEST_FAILED
    assert record_2.ingest_attempts == 1

    assert json.dumps(expected_processing_summary) in caplog.text


def test_base_workflow_finalize_items_already_ingested_item_skipped(
    caplog,
    base_workflow_instance,
    mocked_item_submission_db,
    mocked_sqs_output,
    result_message_attributes,
    result_message_body_success,
    sqs_client,
):
    caplog.set_level("DEBUG")

    ItemSubmissionDB.create(
        item_identifier="10.1002/term.3131",
        batch_id="batch-aaa",
        workflow_name="test",
        status=ItemSubmissionStatus.INGEST_SUCCESS,
    )

    sqs_client.send(
        message_attributes=result_message_attributes,
        message_body=result_message_body_success,
    )

    expected_processing_summary = {
        "received_messages": 1,
        "ingest_success": 0,
        "ingest_failed": 0,
        "ingest_unknown": 0,
    }

    base_workflow_instance.finalize_items()
    assert (
        "Record with primary keys batch_id=batch-aaa (hash key) and "
        "item_identifier=10.1002/term.3131 (range key) already ingested, skipping"
        in caplog.text
    )
    assert json.dumps(expected_processing_summary) in caplog.text


def test_base_workflow_finalize_items_missing_result_message_skipped(
    caplog,
    base_workflow_instance,
    mocked_item_submission_db,
    mocked_sqs_output,
    result_message_attributes,
    result_message_body_success,
    sqs_client,
):
    caplog.set_level("DEBUG")
    ItemSubmissionDB.create(
        item_identifier="10.1002/term.3131",
        batch_id="batch-aaa",
        workflow_name="test",
        status=ItemSubmissionStatus.SUBMIT_SUCCESS,
    )
    ItemSubmissionDB.create(
        item_identifier="10.1002/term.4242",
        batch_id="batch-aaa",
        workflow_name="test",
        status=ItemSubmissionStatus.SUBMIT_SUCCESS,
    )

    sqs_client.send(
        message_attributes=result_message_attributes,
        message_body=result_message_body_success,
    )

    expected_processing_summary = {
        "received_messages": 1,
        "ingest_success": 1,
        "ingest_failed": 0,
        "ingest_unknown": 0,
    }

    base_workflow_instance.finalize_items()
    assert json.dumps(expected_processing_summary) in caplog.text

    record_1 = ItemSubmissionDB.get("batch-aaa", "10.1002/term.3131")
    assert record_1.status == ItemSubmissionStatus.INGEST_SUCCESS
    assert record_1.ingest_attempts == 1

    record_2 = ItemSubmissionDB.get("batch-aaa", "10.1002/term.4242")
    assert record_2.status == ItemSubmissionStatus.SUBMIT_SUCCESS
    assert record_2.ingest_attempts == 0


def test_base_workflow_finalize_items_with_unknown_ingest_result(
    caplog,
    base_workflow_instance,
    mocked_item_submission_db,
    mocked_sqs_output,
    result_message_attributes,
    result_message_body_error,
    sqs_client,
):
    caplog.set_level("DEBUG")

    ItemSubmissionDB.create(
        item_identifier="10.1002/term.4242",
        batch_id="batch-aaa",
        workflow_name="test",
        status=ItemSubmissionStatus.SUBMIT_SUCCESS,
    )
    result_message_attributes["PackageID"]["StringValue"] = "10.1002/term.4242"
    result_message_body_error = json.loads(result_message_body_error)
    result_message_body_error["ResultType"] = "false"

    sqs_client.send(
        message_attributes=result_message_attributes,
        message_body=json.dumps(result_message_body_error),
    )

    base_workflow_instance.finalize_items()

    record = ItemSubmissionDB.get("batch-aaa", "10.1002/term.4242")
    assert record.status == ItemSubmissionStatus.INGEST_UNKNOWN
    assert record.ingest_attempts == 1

    expected_summary = {
        "received_messages": 1,
        "ingest_success": 0,
        "ingest_failed": 0,
        "ingest_unknown": 1,
    }
    assert json.dumps(expected_summary) in caplog.text


def test_base_workflow_finalize_items_exception_handled_and_logged(
    caplog,
    base_workflow_instance,
    mocked_item_submission_db,
    mocked_sqs_output,
    result_message_attributes,
    result_message_body_success,
    sqs_client,
):
    caplog.set_level("DEBUG")
    sqs_client.send(
        message_attributes=result_message_attributes,
        message_body='{"fail": "fail"}',
    )

    base_workflow_instance.finalize_items()

    expected_summary = {
        "received_messages": 0,
        "ingest_success": 0,
        "ingest_failed": 0,
        "ingest_unknown": 0,
    }
    assert "Failure parsing message" in caplog.text
    assert json.dumps(expected_summary) in caplog.text


def test_base_workflow_workflow_specific_processing_success(
    caplog,
    base_workflow_instance,
    mocked_ses,
):
    base_workflow_instance.workflow_specific_processing()
    assert "No extra processing for batch based on workflow: 'test'" in caplog.text


def test_base_workflow_send_report_success(
    caplog,
    base_workflow_instance,
    mocked_ses,
):
    caplog.set_level("DEBUG")
    base_workflow_instance.send_report(
        report_class=FinalizeReport, email_recipients=["test@test.test"]
    )
    assert "Logs sent to ['test@test.test']" in caplog.text
