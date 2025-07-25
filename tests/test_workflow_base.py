import json
from datetime import UTC, datetime
from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError

from dsc.db.models import ItemSubmissionDB, ItemSubmissionStatus
from dsc.exceptions import (
    InvalidDSpaceMetadataError,
    InvalidSQSMessageError,
    InvalidWorkflowNameError,
    ItemMetadatMissingRequiredFieldError,
)
from dsc.item_submission import ItemSubmission
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


def test_base_workflow_reconcile_bitstreams_and_metadata_if_non_reconcile_raises_error(
    base_workflow_instance,
):
    with pytest.raises(
        TypeError,
        match=(
            "Method 'reconcile_bitstreams_and_metadata' not used by workflow 'TestWorkflow'"  # noqa: E501
        ),
    ):
        base_workflow_instance.reconcile_bitstreams_and_metadata()


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


def test_base_workflow_submit_items_missing_row_raises_warning(
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
        status=ItemSubmissionStatus.RECONCILE_SUCCESS,
    )
    items = base_workflow_instance.submit_items(collection_handle="123.4/5678")

    expected_submission_summary = {"total": 2, "submitted": 1, "skipped": 1, "errors": 0}

    assert len(items) == 1
    assert (
        "Record with primary keys batch_id=batch-aaa (hash key) and "
        "item_identifier=789 (range key)not found. Verify that it been reconciled."
        in caplog.text
    )
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


def test_base_workflow_item_submission_iter_success(
    base_workflow_instance, mocked_item_submission_db
):
    ItemSubmissionDB.create(
        item_identifier="123",
        batch_id="batch-aaa",
        workflow_name="test",
        status=ItemSubmissionStatus.RECONCILE_SUCCESS,
    )
    assert next(base_workflow_instance.item_submissions_iter()) == ItemSubmission(
        batch_id="batch-aaa",
        workflow_name="test",
        dspace_metadata={
            "metadata": [
                {"key": "dc.title", "value": "Title", "language": "en_US"},
                {"key": "dc.contributor", "value": "Author 1", "language": None},
                {"key": "dc.contributor", "value": "Author 2", "language": None},
            ]
        },
        bitstream_s3_uris=[
            "s3://dsc/test/batch-aaa/123_01.pdf",
            "s3://dsc/test/batch-aaa/123_02.pdf",
        ],
        item_identifier="123",
        status=ItemSubmissionStatus.RECONCILE_SUCCESS,
        last_run_date=datetime(2025, 1, 1, 9, 0, tzinfo=UTC),
    )


def test_base_workflow_create_dspace_metadata_success(
    base_workflow_instance,
    item_metadata,
):
    item_metadata["topics"] = [
        "Topic Header - Topic Subheading - Topic Name",
        "Topic Header 2 - Topic Subheading 2 - Topic Name 2",
    ]
    assert base_workflow_instance.create_dspace_metadata(item_metadata) == {
        "metadata": [
            {"key": "dc.title", "language": "en_US", "value": "Title"},
            {"key": "dc.contributor", "language": None, "value": "Author 1"},
            {"key": "dc.contributor", "language": None, "value": "Author 2"},
            {
                "key": "dc.subject",
                "language": None,
                "value": "Topic Header - Topic Subheading - Topic Name",
            },
            {
                "key": "dc.subject",
                "language": None,
                "value": "Topic Header 2 - Topic Subheading 2 - Topic Name 2",
            },
        ]
    }


def test_base_workflow_create_dspace_metadata_required_field_missing_raises_exception(
    base_workflow_instance,
    item_metadata,
):
    item_metadata.pop("title")
    with pytest.raises(ItemMetadatMissingRequiredFieldError):
        base_workflow_instance.create_dspace_metadata(item_metadata)


def test_base_workflow_validate_dspace_metadata_success(
    base_workflow_instance,
    dspace_metadata,
):
    assert base_workflow_instance.validate_dspace_metadata(dspace_metadata)


def test_base_workflow_validate_dspace_metadata_invalid_raises_exception(
    base_workflow_instance,
):
    with pytest.raises(InvalidDSpaceMetadataError):
        base_workflow_instance.validate_dspace_metadata({})


@patch("dsc.db.models.ItemSubmissionDB.get")
def test_base_workflow_process_result_messages_success(
    mock_item_submission_db_get,
    caplog,
    base_workflow_instance,
    mocked_item_submission_db,
    mocked_sqs_output,
    result_message_attributes,
    result_message_body,
    sqs_client,
):
    caplog.set_level("DEBUG")

    mock_item_submission_db_get.return_value = ItemSubmissionDB(
        batch_id="batch-aaa", item_identifier="10.1002/term.3131", workflow_name="test"
    )

    sqs_client.send(
        message_attributes=result_message_attributes,
        message_body=result_message_body,
    )

    expected_processing_summary = {
        "received_messages": 1,
        "ingest_success": 1,
        "ingest_failed": 0,
        "ingest_unknown": 0,
    }

    base_workflow_instance.process_result_messages()

    assert "Item was ingested" in caplog.text
    assert json.dumps(expected_processing_summary) in caplog.text


def test_base_workflow_process_result_messages_if_invalid_msg_attrs_log(
    caplog,
    base_workflow_instance,
    mocked_sqs_output,
    sqs_client,
):
    caplog.set_level("DEBUG")

    # send message with invalid 'MessageAttributes' to 'mock-output-queue'
    invalid_result_message_attrs = {
        "SubmissionSource": {"DataType": "String", "StringValue": "Submission system"},
    }
    sqs_client.send(
        message_attributes=invalid_result_message_attrs,
        message_body="",
    )

    expected_processing_summary = {
        "received_messages": 1,
        "ingest_success": 0,
        "ingest_failed": 0,
        "ingest_unknown": 1,
    }
    base_workflow_instance.process_result_messages()

    assert "Failed to parse 'MessageAttributes'" in caplog.text
    assert json.dumps(expected_processing_summary) in caplog.text


@patch("dsc.db.models.ItemSubmissionDB.get")
def test_base_workflow_process_result_messages_if_invalid_msg_body_log_and_capture(
    mock_item_submission_db_get,
    caplog,
    base_workflow_instance,
    mocked_item_submission_db,
    mocked_sqs_output,
    result_message_attributes,
    sqs_client,
):
    caplog.set_level("DEBUG")

    mock_item_submission_db_get.return_value = ItemSubmissionDB(
        batch_id="batch-aaa", item_identifier="10.1002/term.3131", workflow_name="test"
    )

    # send message with invalid 'Body' to 'mock-output-queue'
    sqs_client.send(
        message_attributes=result_message_attributes,
        message_body="",
    )

    expected_processing_summary = {
        "received_messages": 1,
        "ingest_success": 0,
        "ingest_failed": 0,
        "ingest_unknown": 1,
    }
    base_workflow_instance.process_result_messages()

    assert "Failed to parse content of 'Body'" in caplog.text
    assert json.dumps(expected_processing_summary) in caplog.text


@patch("dsc.db.models.ItemSubmissionDB.get")
@patch("dsc.workflows.Workflow._parse_result_message_body")
def test_base_workflow_process_result_messages_if_ingest_failed_log_and_capture(
    mock_workflow_parse_result_message_body,
    mock_item_submission_db_get,
    caplog,
    base_workflow_instance,
    mocked_item_submission_db,
    mocked_sqs_output,
    result_message_attributes,
    sqs_client,
):
    caplog.set_level("DEBUG")

    # mock "Body" for failed ingest
    ingest_failed_result_message_body = {
        "ResultType": "error",
        "ErrorTimestamp": "text",
        "ErrorInfo": "text",
        "DSpaceResponse": "text",
        "ExceptionTraceback": ["text"],
    }
    mock_workflow_parse_result_message_body.return_value = (
        ingest_failed_result_message_body
    )

    mock_item_submission_db_get.return_value = ItemSubmissionDB(
        batch_id="batch-aaa", item_identifier="10.1002/term.3131", workflow_name="test"
    )

    sqs_client.send(
        message_attributes=result_message_attributes,
        message_body=json.dumps(ingest_failed_result_message_body),
    )

    expected_processing_summary = {
        "received_messages": 1,
        "ingest_success": 0,
        "ingest_failed": 1,
        "ingest_unknown": 0,
    }
    base_workflow_instance.process_result_messages()

    assert "Item failed ingest" in caplog.text
    assert json.dumps(expected_processing_summary) in caplog.text


def test_base_workflow_parse_result_message_attrs_success(
    base_workflow_instance, result_message_valid
):
    message_attributes = result_message_valid["MessageAttributes"]
    valid_message_attributes = (
        base_workflow_instance._parse_result_message_attrs(  # noqa: SLF001
            message_attributes=message_attributes
        )
    )

    assert valid_message_attributes == message_attributes


def test_base_workflow_parse_result_message_attrs_if_json_schema_validation_fails(
    base_workflow_instance, result_message_valid
):
    message_attributes = result_message_valid["MessageAttributes"]

    # modify content of 'MessageAttributes'
    del message_attributes["SubmissionSource"]

    with pytest.raises(
        InvalidSQSMessageError,
        match="Content of 'MessageAttributes' failed schema validation",
    ):
        base_workflow_instance._parse_result_message_attrs(  # noqa: SLF001
            message_attributes=message_attributes
        )


def test_base_workflow_parse_result_message_body_success(
    base_workflow_instance, result_message_valid, result_message_body
):
    message_body = result_message_valid["Body"]
    valid_message_body = (
        base_workflow_instance._parse_result_message_body(  # noqa: SLF001
            message_body=message_body
        )
    )

    assert valid_message_body == json.loads(result_message_body)


def test_base_workflow_parse_result_message_body_if_json_schema_validation_fails(
    base_workflow_instance, result_message_valid
):
    original_message_body = result_message_valid["Body"]

    # modify content of 'Body'
    modified_message_body = json.loads(original_message_body)
    del modified_message_body["Bitstreams"]

    with pytest.raises(
        InvalidSQSMessageError, match="Content of 'Body' failed schema validation"
    ):
        base_workflow_instance._parse_result_message_body(  # noqa: SLF001
            message_body=json.dumps(modified_message_body),
        )


def test_base_workflow_parse_result_message_body_if_invalid_json(base_workflow_instance):
    with pytest.raises(InvalidSQSMessageError, match="Failed to parse content of 'Body'"):
        base_workflow_instance._parse_result_message_body(message_body="")  # noqa: SLF001


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
