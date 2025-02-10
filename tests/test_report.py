from unittest.mock import patch

from freezegun import freeze_time

from dsc.reports import FinalizeReport
from dsc.workflows.base import WorkflowEvents


@freeze_time("2025-01-01 09:00:00")
def test_finalize_report_init_success(workflow_events_finalize):
    finalize_report = FinalizeReport(
        workflow_name="test", batch_id="aaa", events=workflow_events_finalize
    )

    assert finalize_report.workflow_name == "test"
    assert finalize_report.batch_id == "aaa"
    assert finalize_report.report_date == "2025-01-01 09:00:00"
    assert finalize_report.events == workflow_events_finalize


@freeze_time("2025-01-01 09:00:00")
def test_finalize_report_init_from_workflow_success(
    base_workflow_instance, workflow_events_finalize
):
    finalize_report = FinalizeReport.from_workflow(base_workflow_instance)

    assert finalize_report.workflow_name == base_workflow_instance.workflow_name
    assert finalize_report.batch_id == base_workflow_instance.batch_id
    assert finalize_report.report_date == "2025-01-01 09:00:00"
    assert isinstance(finalize_report.events, WorkflowEvents)


def test_finalize_report_subject_success(workflow_events_finalize):
    finalize_report = FinalizeReport(
        workflow_name="test", batch_id="aaa", events=workflow_events_finalize
    )
    assert finalize_report.subject == "DSpace Submission Results - test, batch='aaa'"


@patch("dsc.reports.FinalizeReport._write_errors_text_file")
@patch("dsc.reports.FinalizeReport._write_ingested_items_csv")
def test_finalize_report_create_attachments_success(
    mock_finalize_report_processed_items_csv,
    mock_finalize_report_errors_txt,
    workflow_events_finalize,
):
    mock_finalize_report_processed_items_csv.return_value = "processed items csv content"
    mock_finalize_report_errors_txt.return_value = "errors txt content"
    workflow_events_finalize.errors = ["This is an error"]

    finalize_report = FinalizeReport(
        workflow_name="test", batch_id="aaa", events=workflow_events_finalize
    )
    attachments = finalize_report.create_attachments()

    assert attachments == [
        ("ingested_items.csv", "processed items csv content"),
        ("errors.txt", "errors txt content"),
    ]
