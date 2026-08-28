from unittest.mock import patch

import pytest

from dsc.exceptions import InvalidWorkflowNameError
from dsc.workflows.base import Workflow


def test_workflow_get_workflow_success():
    # get workflow by name
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


def test_workflow_get_workflow_invalid_workflow_name_raises_error():
    with pytest.raises(InvalidWorkflowNameError):
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
