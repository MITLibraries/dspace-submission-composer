from unittest.mock import patch

from freezegun import freeze_time

from dsc.reports import FinalizeReport, ReconcileReport
from dsc.workflows.base import WorkflowEvents


@freeze_time("2025-01-01 09:00:00")
def test_reconcile_report_init_success(workflow_events_reconcile):
    reconcile_report = ReconcileReport(
        workflow_name="test", batch_id="aaa", events=workflow_events_reconcile
    )

    assert reconcile_report.workflow_name == "test"
    assert reconcile_report.batch_id == "aaa"
    assert reconcile_report.report_date == "2025-01-01 09:00:00"
    assert reconcile_report.events == workflow_events_reconcile


def test_reconcile_report_subject_success(workflow_events_reconcile):
    reconcile_report = ReconcileReport(
        workflow_name="test", batch_id="aaa", events=workflow_events_reconcile
    )
    assert reconcile_report.subject == "DSC Reconcile Results - test, batch='aaa'"


def test_reconcile_report_create_attachments_success(workflow_events_reconcile):
    reconcile_report = ReconcileReport(
        workflow_name="test", batch_id="aaa", events=workflow_events_reconcile
    )
    attachments = reconcile_report.create_attachments()

    reconciled_items_filename, reconciled_items_buffer = attachments[0]
    assert reconciled_items_filename == "reconciled_items.txt"
    assert reconciled_items_buffer.readlines() == [
        "Reconciled items (item_identifier -> bitstreams)\n",
        "\n",
        "1. 123 -> ['123.pdf', '123.tiff']\n",
    ]

    reconcile_error_filename_a, reconcile_error_buffer_a = attachments[1]
    assert reconcile_error_filename_a == "bitstreams_without_metadata.txt"
    assert reconcile_error_buffer_a.readlines() == [
        "Bitstreams without metadata\n",
        "\n",
        "1. 124.pdf\n",
    ]

    reconcile_error_filename_b, reconcile_error_buffer_b = attachments[2]
    assert reconcile_error_filename_b == "metadata_without_bitstreams.txt"
    assert reconcile_error_buffer_b.readlines() == [
        "Metadata without bitstreams\n",
        "\n",
        "1. 125\n",
    ]


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
