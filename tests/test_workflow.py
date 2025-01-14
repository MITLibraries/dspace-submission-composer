from unittest.mock import patch

import pytest

from dsc.exceptions import (
    InvalidDSpaceMetadataError,
    InvalidWorkflowNameError,
    ItemMetadatMissingRequiredFieldError,
)
from dsc.item_submission import ItemSubmission
from dsc.workflows.base import Workflow


def test_base_workflow_init_with_defaults_success():
    workflow_class = Workflow.get_workflow(workflow_name="test")
    workflow_instance = workflow_class(
        collection_handle="123.4/5678",
        batch_id="batch-aaa",
        email_recipients=("test@test.test",),
    )
    assert workflow_instance.workflow_name == "test"
    assert workflow_instance.submission_system == "Test@MIT"
    assert (
        workflow_instance.metadata_mapping_path
        == "tests/fixtures/test_metadata_mapping.json"
    )
    assert workflow_instance.s3_bucket == "dsc"
    assert workflow_instance.output_queue == "dsc-unhandled"
    assert workflow_instance.collection_handle == "123.4/5678"
    assert workflow_instance.batch_id == "batch-aaa"
    assert workflow_instance.email_recipients == ("test@test.test",)


def test_base_workflow_init_with_optional_params_success():
    workflow_class = Workflow.get_workflow(workflow_name="test")
    workflow_instance = workflow_class(
        collection_handle="123.4/5678",
        batch_id="batch-aaa",
        email_recipients=("test@test.test",),
        s3_bucket="updated-bucket",
        output_queue="mock-output_queue",
    )
    assert workflow_instance.workflow_name == "test"
    assert workflow_instance.submission_system == "Test@MIT"
    assert (
        workflow_instance.metadata_mapping_path
        == "tests/fixtures/test_metadata_mapping.json"
    )
    assert workflow_instance.s3_bucket == "updated-bucket"
    assert workflow_instance.output_queue == "mock-output_queue"
    assert workflow_instance.collection_handle == "123.4/5678"
    assert workflow_instance.batch_id == "batch-aaa"
    assert workflow_instance.email_recipients == ("test@test.test",)


def test_base_workflow_get_workflow_success():
    workflow_class = Workflow.get_workflow("test")
    assert workflow_class.workflow_name == "test"


def test_base_workflow_get_workflow_invalid_workflow_name_raises_error(
    base_workflow_instance,
):
    with pytest.raises(InvalidWorkflowNameError):
        base_workflow_instance.get_workflow("tast")


def test_base_workflow_reconcile_bitstreams_and_metadata_success(
    caplog, base_workflow_instance, mocked_s3, s3_client
):
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/123_01.pdf")
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/123_02.jpg")
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/456_01.pdf")
    assert base_workflow_instance.reconcile_bitstreams_and_metadata() == (
        {"789"},
        {"456"},
    )
    assert (
        "Item identifiers from batch metadata with matching bitstreams: ['123']"
        in caplog.text
    )


def test_build_bitstream_dict_success(mocked_s3, s3_client, base_workflow_instance):
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/123_01.pdf")
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/123_02.pdf")
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/456_01.pdf")
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/789_01.jpg")
    assert base_workflow_instance._build_bitstream_dict() == {  # noqa: SLF001
        "123": ["test/batch-aaa/123_01.pdf", "test/batch-aaa/123_02.pdf"],
        "456": ["test/batch-aaa/456_01.pdf"],
        "789": ["test/batch-aaa/789_01.jpg"],
    }


def test_match_item_identifiers_to_bitstreams_success(base_workflow_instance):
    bitstream_dict = {"test": "test_01.pdf"}
    item_identifiers = ["test", "tast"]
    item_identifier_matches = (
        base_workflow_instance._match_item_identifiers_to_bitstreams(  # noqa: SLF001
            bitstream_dict.keys(), item_identifiers
        )
    )
    assert len(item_identifier_matches) == 1
    assert "test" in item_identifier_matches


def test_match_bitstreams_to_item_identifiers_success(base_workflow_instance):
    bitstream_dict = {"test": "test_01.pdf", "tast": "tast_01.pdf"}
    item_identifiers = ["test"]
    file_matches = (
        base_workflow_instance._match_bitstreams_to_item_identifiers(  # noqa: SLF001
            bitstream_dict, item_identifiers
        )
    )
    assert len(file_matches) == 1
    assert "test" in file_matches


def test_base_workflow_run_success(
    caplog, base_workflow_instance, mocked_s3, mocked_sqs_input, mocked_sqs_output
):
    caplog.set_level("DEBUG")
    submission_results = base_workflow_instance.run()
    assert "Processing submission for '123'" in caplog.text
    assert (
        "Metadata uploaded to S3: s3://dsc/test/batch-aaa/123_metadata.json"
        in caplog.text
    )
    assert submission_results["success"] is True
    assert submission_results["items_count"] == 2  # noqa: PLR2004
    assert len(submission_results["items"]["succeeded"]) == 2  # noqa: PLR2004
    assert not submission_results["items"]["failed"]


@patch("dsc.item_submission.ItemSubmission.send_submission_message")
def test_base_workflow_run_exceptions_handled(
    mocked_method,
    caplog,
    base_workflow_instance,
    mocked_s3,
    mocked_sqs_input,
    mocked_sqs_output,
):
    side_effect = [
        {"MessageId": "abcd", "ResponseMetadata": {"HTTPStatusCode": 200}},
        Exception,
    ]
    mocked_method.side_effect = side_effect
    submission_results = base_workflow_instance.run()
    assert submission_results["success"] is False
    assert submission_results["items_count"] == 2  # noqa: PLR2004
    assert submission_results["items"]["succeeded"] == {"123": "abcd"}
    assert isinstance(submission_results["items"]["failed"]["789"], Exception)


@patch("dsc.item_submission.ItemSubmission.send_submission_message")
def test_base_workflow_run_invalid_status_codes_handled(
    mocked_method,
    caplog,
    base_workflow_instance,
    mocked_s3,
    mocked_sqs_input,
    mocked_sqs_output,
):
    side_effect = [
        {"MessageId": "abcd", "ResponseMetadata": {"HTTPStatusCode": 200}},
        {"ResponseMetadata": {"HTTPStatusCode": 400}},
    ]
    mocked_method.side_effect = side_effect
    submission_results = base_workflow_instance.run()
    assert submission_results["success"] is False
    assert submission_results["items_count"] == 2  # noqa: PLR2004
    assert submission_results["items"]["succeeded"] == {"123": "abcd"}
    assert isinstance(submission_results["items"]["failed"]["789"], RuntimeError)


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
