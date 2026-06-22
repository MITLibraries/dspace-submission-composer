# ruff: noqa: TD002, TD003, FIX002
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
    assert create_report.report_date == "20250101T090000Z"
    assert create_report.subject == (
        "[test] DSC Create Batch Results - test, batch='batch-aaa'"
    )


def test_report_get_item_submissions(mock_item_submission_db_with_records):
    create_report = CreateReport(workflow_name="test", batch_id="aaa")
    item_submissions = create_report.get_item_submissions()

    assert isinstance(item_submissions, list)
    assert len(item_submissions) == 2  # noqa: PLR2004


def test_report_filter_item_submissions_by_status(mock_item_submission_db_with_records):
    # TODO: Update fixture after all Workflows are updated to longer use
    #       ItemSubmissionStatus.BATCH_CREATED
    create_report = CreateReport(workflow_name="test", batch_id="aaa")
    item_submissions = create_report.filter_item_submissions_by_status(
        ItemSubmissionStatus.INGEST_SUCCESS
    )

    assert len(item_submissions) == 0


def test_report_generate_summary(mock_item_submission_db_with_records):
    create_report = CreateReport(workflow_name="test", batch_id="aaa")

    assert "Created: 2" in create_report.generate_summary()


def test_report_prepare_attachments(mock_item_submission_db_with_records):
    create_report = CreateReport(workflow_name="test", batch_id="aaa")
    attachment = create_report.prepare_attachments()[0]

    assert attachment[0] == "aaa-item-submissions.csv"
    assert isinstance(attachment[1], StringIO)


def test_report_prepare_attachments_with_batch_creation_errors(mock_item_submission_db):
    create_report = CreateReport(
        workflow_name="test",
        batch_id="aaa",
        errors=[("123", str(Exception("An error occurred")))],
    )
    attachments = create_report.prepare_attachments()

    assert len(attachments) == 1  # only an errors CSV is sent
    assert attachments[0][0] == "aaa-errors.csv"


def test_report_upload_attachments(mock_item_submission_db_with_records, tmp_path):
    create_report = CreateReport(workflow_name="test", batch_id="aaa")
    create_report.upload_attachments(output_location=str(tmp_path))

    assert os.path.exists(tmp_path / "aaa-item-submissions.csv")


def test_report_create_item_submissions_csv(mock_item_submission_db_with_records):
    create_report = CreateReport(workflow_name="test", batch_id="aaa")
    output = create_report.create_item_submissions_csv()

    assert isinstance(output, StringIO)


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
