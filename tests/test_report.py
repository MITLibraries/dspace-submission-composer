import os
from io import StringIO

from freezegun import freeze_time

from dsc.reports import CreateBatchReport, FinalizeReport, ReconcileReport, SubmitReport
from dsc.workflows.base import WorkflowEvents


@freeze_time("2025-01-01 09:00:00")
def test_create_batch_report_init_success():
    create_batch_report = CreateBatchReport(workflow_name="test", batch_id="aaa")

    assert create_batch_report.workflow_name == "test"
    assert create_batch_report.batch_id == "aaa"
    assert create_batch_report.report_date == "2025-01-01 09:00:00"
    assert create_batch_report.subject == ("DSC Create Batch Results - test, batch='aaa'")


def test_create_batch_report_retrieves_all_item_submissions(
    mock_item_submission_db_with_records,
):
    create_batch_report = CreateBatchReport(workflow_name="test", batch_id="aaa")

    assert create_batch_report.item_submissions == [
        {
            "batch_id": "aaa",
            "item_identifier": "123",
            "source_system_identifier": None,
            "status": "batch_created",
            "status_details": None,
            "dspace_handle": None,
            "ingest_date": None,
        },
        {
            "batch_id": "aaa",
            "item_identifier": "456",
            "source_system_identifier": None,
            "status": "batch_created",
            "status_details": None,
            "dspace_handle": None,
            "ingest_date": None,
        },
    ]


def test_create_batch_report_generate_summary(mock_item_submission_db_with_records):
    create_batch_report = CreateBatchReport(workflow_name="test", batch_id="aaa")

    assert "Created: 2" in create_batch_report.generate_summary()


def test_create_batch_report_write_to_csv_file(
    mock_item_submission_db_with_records, tmp_path
):
    create_batch_report = CreateBatchReport(workflow_name="test", batch_id="aaa")
    create_batch_report.write_to_csv(output_file=tmp_path / "data.csv")

    assert os.path.exists(tmp_path / "data.csv")


def test_create_batch_report_write_to_csv_buffer(
    mock_item_submission_db_with_records, tmp_path
):
    csv_buffer = StringIO()
    create_batch_report = CreateBatchReport(workflow_name="test", batch_id="aaa")
    create_batch_report.write_to_csv(output_file=csv_buffer)

    csv_buffer.seek(0)
    assert csv_buffer.read()


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
    assert reconciled_items_filename == "reconciled_items.csv"
    assert reconciled_items_buffer.readlines() == [
        "item_identifier,bitstreams\n",
        "123,\"['123.pdf', '123.tiff']\"\n",
    ]

    reconcile_error_filename_a, reconcile_error_buffer_a = attachments[1]
    assert reconcile_error_filename_a == "bitstreams_without_metadata.csv"
    assert reconcile_error_buffer_a.readlines() == ["bitstream\n", "124.pdf\n"]

    reconcile_error_filename_b, reconcile_error_buffer_b = attachments[2]
    assert reconcile_error_filename_b == "metadata_without_bitstreams.csv"
    assert reconcile_error_buffer_b.readlines() == [
        "item_identifier\n",
        "125\n",
    ]


@freeze_time("2025-01-01 09:00:00")
def test_submit_report_init_success(workflow_events_submit):
    submit_report = SubmitReport(
        workflow_name="test", batch_id="aaa", events=workflow_events_submit
    )

    assert submit_report.workflow_name == "test"
    assert submit_report.batch_id == "aaa"
    assert submit_report.report_date == "2025-01-01 09:00:00"
    assert submit_report.events == workflow_events_submit


def test_submit_report_subject_success(workflow_events_submit):
    submit_report = SubmitReport(
        workflow_name="test", batch_id="aaa", events=workflow_events_submit
    )
    assert submit_report.subject == "DSC Submission Results - test, batch='aaa'"


def test_submit_report_create_attachments_success(workflow_events_submit):
    submit_report = SubmitReport(
        workflow_name="test", batch_id="aaa", events=workflow_events_submit
    )
    attachments = submit_report.create_attachments()

    submitted_items_filename, submitted_items_buffer = attachments[0]
    assert submitted_items_filename == "submitted_items.csv"
    assert submitted_items_buffer.readlines() == [
        "item_identifier,message_id\n",
        "123,abc\n",
    ]

    errors_filename, errors_buffer = attachments[1]
    assert errors_filename == "errors.csv"
    assert errors_buffer.readlines() == [
        "error\n",
        "Failed to send submission message for item: 124\n",
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


def test_finalize_report_create_attachments_success(workflow_events_finalize):
    finalize_report = FinalizeReport(
        workflow_name="test", batch_id="aaa", events=workflow_events_finalize
    )
    attachments = finalize_report.create_attachments()

    ingested_items_filename, ingested_items_buffer = attachments[0]
    assert ingested_items_filename == "dss_submission_results.csv"
    assert (
        ingested_items_buffer.readlines()[0]
        == "item_identifier,ingested,dspace_handle,error,result_message_body\n"
    )
