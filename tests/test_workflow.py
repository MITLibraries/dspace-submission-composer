import json
from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError

from dsc.exceptions import (
    InvalidDSpaceMetadataError,
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
    caplog, base_workflow_instance, mocked_s3, mocked_sqs_input, mocked_sqs_output
):
    caplog.set_level("DEBUG")
    items = base_workflow_instance.submit_items("123.4/5678")

    expected_submission_summary = {"total": 2, "submitted": 2, "errors": 0}

    assert len(items) == 2  # noqa: PLR2004
    assert json.dumps(expected_submission_summary) in caplog.text


@patch("dsc.item_submission.ItemSubmission.send_submission_message")
def test_base_workflow_submit_items_exceptions_handled(
    mocked_method,
    caplog,
    base_workflow_instance,
    mocked_s3,
    mocked_sqs_input,
    mocked_sqs_output,
):
    side_effect = [
        {"MessageId": "abcd", "ResponseMetadata": {"HTTPStatusCode": 200}},
        ClientError,
    ]
    mocked_method.side_effect = side_effect
    items = base_workflow_instance.submit_items("123.4/5678")

    expected_submission_summary = {"total": 2, "submitted": 1, "errors": 1}

    assert len(items) == 1
    assert items == [{"item_identifier": "123", "message_id": "abcd"}]
    assert json.dumps(expected_submission_summary) in caplog.text


def test_base_workflow_item_submission_iter_success(base_workflow_instance):
    assert next(base_workflow_instance.item_submissions_iter()) == ItemSubmission(
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
    )


def test_base_workflow_create_dspace_metadata_success(
    base_workflow_instance,
    item_metadata,
):
    assert base_workflow_instance.create_dspace_metadata(item_metadata) == {
        "metadata": [
            {"key": "dc.title", "language": "en_US", "value": "Title"},
            {"key": "dc.contributor", "language": None, "value": "Author 1"},
            {"key": "dc.contributor", "language": None, "value": "Author 2"},
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


def test_base_workflow_process_sqs_queue_success(
    caplog,
    base_workflow_instance,
    mocked_sqs_output,
    result_message_attributes,
    result_message_body,
    sqs_client,
):
    caplog.set_level("DEBUG")
    sqs_client.send(
        message_attributes=result_message_attributes,
        message_body=result_message_body,
    )

    expected_processing_summary = {"total": 1, "ingested": 1, "errors": 0}
    items = base_workflow_instance.process_sqs_queue()

    assert base_workflow_instance.workflow_events.processed_items[0]["ingested"]
    assert "Item was successfully ingested: 10.1002/term.3131" in caplog.text
    assert json.dumps(expected_processing_summary) in caplog.text
    assert items == [
        {
            "item_identifier": "10.1002/term.3131",
            "result_message_body": {
                "Bitstreams": [
                    {
                        "BitstreamChecksum": {
                            "checkSumAlgorithm": "MD5",
                            "value": "a4e0f4930dfaff904fa3c6c85b0b8ecc",
                        },
                        "BitstreamName": "10.1002-term.3131.pdf",
                        "BitstreamUUID": "a1b2c3d4e5",
                    }
                ],
                "ItemHandle": "1721.1/131022",
                "ResultType": "success",
                "lastModified": "Thu Sep 09 17:56:39 UTC 2021",
            },
            "ingested": True,
        }
    ]


@patch("dsc.utilities.aws.sqs.SQSClient.parse_dss_result_message")
def test_base_workflow_process_sqs_queue_if_exception_capture_event_and_log(
    mocked_workflow_process_result_message,
    caplog,
    base_workflow_instance,
    mocked_sqs_output,
    result_message_attributes,
    result_message_body,
    sqs_client,
):
    mocked_workflow_process_result_message.side_effect = [Exception("An error occurred")]
    caplog.set_level("DEBUG")
    sqs_client.send(
        message_attributes=result_message_attributes,
        message_body=result_message_body,
    )

    expected_processing_summary = {
        "total": 1,
        "ingested": 0,
        "errors": 1,
    }
    items = base_workflow_instance.process_sqs_queue()

    assert base_workflow_instance.workflow_events.errors[0] == "An error occurred"
    assert "An error occurred" in caplog.text
    assert json.dumps(expected_processing_summary) in caplog.text
    assert items == []


@patch("dsc.utilities.aws.sqs.SQSClient.parse_dss_result_message")
def test_base_workflow_process_sqs_queue_if_not_ingested_capture_event_and_log(
    mocked_workflow_process_result_message,
    caplog,
    base_workflow_instance,
    mocked_sqs_output,
    result_message_attributes,
    sqs_client,
):
    result_message_body = {"ResultType": "error"}
    mocked_workflow_process_result_message.return_value = ("123", result_message_body)
    caplog.set_level("DEBUG")
    sqs_client.send(
        message_attributes=result_message_attributes,
        message_body=json.dumps(result_message_body),
    )

    expected_processing_summary = {
        "total": 1,
        "ingested": 0,
        "errors": 1,
    }
    items = base_workflow_instance.process_sqs_queue()

    assert not base_workflow_instance.workflow_events.processed_items[0]["ingested"]
    assert (
        "Item was not ingested: 123" in base_workflow_instance.workflow_events.errors[0]
    )
    assert "Item was not ingested: 123" in caplog.text
    assert json.dumps(expected_processing_summary) in caplog.text
    assert items == [
        {
            "item_identifier": "123",
            "result_message_body": result_message_body,
            "ingested": False,
        }
    ]


def test_base_workflow_workflow_specific_processing_success(
    caplog,
    base_workflow_instance,
    mocked_ses,
):
    base_workflow_instance.workflow_specific_processing([{}])
    assert "No extra processing for 1 items based on workflow: 'test'" in caplog.text


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
