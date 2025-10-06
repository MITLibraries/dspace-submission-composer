import os
from io import StringIO

from freezegun import freeze_time

from dsc.db.models import ItemSubmissionStatus
from dsc.item_submission import ItemSubmission
from dsc.reports import CreateReport, FinalizeReport, SubmitReport


@freeze_time("2025-01-01 09:00:00")
def test_report_init_success():
    create_report = CreateReport(workflow_name="test", batch_id="batch-aaa")

    assert create_report.workflow_name == "test"
    assert create_report.batch_id == "batch-aaa"
    assert create_report.report_date == "2025-01-01 09:00:00"
    assert create_report.subject == ("DSC Create Batch Results - test, batch='batch-aaa'")


@freeze_time("2025-01-01 09:00:00")
def test_report_init_from_workflow_success(base_workflow_instance):

    create_report = CreateReport.from_workflow(base_workflow_instance)

    assert create_report.workflow_name == "test"
    assert create_report.batch_id == "batch-aaa"
    assert create_report.report_date == "2025-01-01 09:00:00"
    assert create_report.subject == ("DSC Create Batch Results - test, batch='batch-aaa'")


def test_report_batch_item_submissions_success(
    mock_item_submission_db_with_records,
):
    create_report = CreateReport(workflow_name="test", batch_id="aaa")
    batch_item_submissions = create_report.get_batch_item_submissions()

    assert len(batch_item_submissions) == 2  # noqa: PLR2004


def test_report_write_to_csv_file(mock_item_submission_db_with_records, tmp_path):
    create_report = CreateReport(workflow_name="test", batch_id="aaa")
    create_report.write_item_submissions_csv(output_file=tmp_path / "data.csv")

    assert os.path.exists(tmp_path / "data.csv")


def test_report_write_to_csv_buffer(mock_item_submission_db_with_records, tmp_path):
    csv_buffer = StringIO()
    create_report = CreateReport(workflow_name="test", batch_id="aaa")
    create_report.write_item_submissions_csv(output_file=csv_buffer)

    csv_buffer.seek(0)
    assert csv_buffer.read()


def test_create_report_generate_summary(mock_item_submission_db_with_records):
    create_report = CreateReport(workflow_name="test", batch_id="aaa")

    assert "Created: 2" in create_report.generate_summary()


def test_submit_report_generate_summary(mock_item_submission_db_with_records):
    submit_report = SubmitReport(workflow_name="test", batch_id="aaa")

    # change the status of records in mock DynamoDB table
    item_submission = ItemSubmission.get(batch_id="aaa", item_identifier="123")
    item_submission.status = ItemSubmissionStatus.SUBMIT_SUCCESS
    item_submission.upsert_db()

    assert "Submitted to DSS: 1" in submit_report.generate_summary()


def test_finalize_report_generate_summary(mock_item_submission_db_with_records):
    finalize_report = FinalizeReport(workflow_name="test", batch_id="aaa")

    # change the status of records in mock DynamoDB table
    item_submission = ItemSubmission.get(batch_id="aaa", item_identifier="123")
    item_submission.status = ItemSubmissionStatus.INGEST_SUCCESS
    item_submission.upsert_db()

    assert "Ingested to DSpace: 1" in finalize_report.generate_summary()
