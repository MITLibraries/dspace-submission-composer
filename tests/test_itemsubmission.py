from datetime import UTC, datetime
from http import HTTPStatus

from freezegun import freeze_time

from dsc.db.models import ItemSubmissionDB, ItemSubmissionStatus


def test_itemsubmission_init_success(item_submission_instance, dspace_metadata):
    assert item_submission_instance.dspace_metadata == dspace_metadata
    assert item_submission_instance.bitstream_s3_uris == [
        "s3://dsc/workflow/folder/123_01.pdf",
        "s3://dsc/workflow/folder/123_02.pdf",
    ]
    assert item_submission_instance.item_identifier == "123"


def test_upload_dspace_metadata(mocked_s3, item_submission_instance, s3_client):
    item_submission_instance.upload_dspace_metadata("dsc", "workflow/folder/")
    assert (
        item_submission_instance.metadata_s3_uri
        == "s3://dsc/workflow/folder/dspace_metadata/123_metadata.json"
    )
    response = s3_client.client.get_object(
        Bucket="dsc", Key="workflow/folder/dspace_metadata/123_metadata.json"
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == HTTPStatus.OK


def test_send_submission_message(
    mocked_sqs_input, mocked_sqs_output, item_submission_instance
):
    response = item_submission_instance.send_submission_message(
        "workflow",
        "mock-output-queue",
        "DSpace@MIT",
        "1234/5678",
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == HTTPStatus.OK


def test_populate_from_db_success(item_submission_instance, mocked_item_submission_db):
    test_record = ItemSubmissionDB(
        batch_id=item_submission_instance.batch_id,
        item_identifier=item_submission_instance.item_identifier,
        workflow_name="test",
        status=ItemSubmissionStatus.RECONCILE_SUCCESS,
        status_details="Test details",
        submit_attempts=1,
        ingest_attempts=1,
    )
    test_record.save()
    item_submission_instance.populate_from_db()

    assert item_submission_instance.status == ItemSubmissionStatus.RECONCILE_SUCCESS
    assert item_submission_instance.status_details == "Test details"
    assert item_submission_instance.submit_attempts == 1
    assert item_submission_instance.ingest_attempts == 1
    assert item_submission_instance.workflow_name == "test"


@freeze_time("2025-01-01 09:00:00")
def test_populate_from_db_with_datetime_success(
    item_submission_instance, mocked_item_submission_db
):
    test_date = datetime.now(UTC)
    test_record = ItemSubmissionDB(
        batch_id=item_submission_instance.batch_id,
        item_identifier=item_submission_instance.item_identifier,
        workflow_name="test",
        ingest_date=test_date,
        last_run_date=test_date,
    )
    test_record.save()

    item_submission_instance.populate_from_db()

    assert item_submission_instance.ingest_date == test_date.isoformat()
    assert item_submission_instance.last_run_date == test_date.isoformat()


def test_update_db_success(item_submission_instance, mocked_item_submission_db):
    item_submission_instance.status = ItemSubmissionStatus.RECONCILE_SUCCESS
    item_submission_instance.status_details = "Test update"
    item_submission_instance.submit_attempts = 0

    item_submission_instance.update_db()

    record = ItemSubmissionDB.get(
        hash_key=item_submission_instance.batch_id,
        range_key=item_submission_instance.item_identifier,
    )
    assert record.status == ItemSubmissionStatus.RECONCILE_SUCCESS
    assert record.status_details == "Test update"
    assert record.submit_attempts == 0
    assert record.workflow_name == item_submission_instance.workflow_name


def test_update_db_excludes_metadata_and_bitstreams(
    item_submission_instance, mocked_item_submission_db
):
    item_submission_instance.dspace_metadata = {"title": "Test Item"}
    item_submission_instance.bitstream_s3_uris = [
        "s3://dsc/workflow/folder/123_01.pdf",
        "s3://dsc/workflow/folder/123_02.pdf",
    ]
    item_submission_instance.update_db()
    record = ItemSubmissionDB.get(
        hash_key=item_submission_instance.batch_id,
        range_key=item_submission_instance.item_identifier,
    )
    assert hasattr(record, "dspace_metadata") is False
    assert hasattr(record, "bitstream_s3_uris") is False


def test_ready_to_submit_with_none_status(item_submission_instance):
    item_submission_instance.status = None
    assert item_submission_instance.ready_to_submit() is False


def test_ready_to_submit_with_reconcile_failed(item_submission_instance):
    item_submission_instance.status = ItemSubmissionStatus.RECONCILE_FAILED
    assert item_submission_instance.ready_to_submit() is False


def test_ready_to_submit_with_ingest_success(item_submission_instance):
    item_submission_instance.status = ItemSubmissionStatus.INGEST_SUCCESS
    assert item_submission_instance.ready_to_submit() is False


def test_ready_to_submit_with_submit_success(item_submission_instance):
    item_submission_instance.status = ItemSubmissionStatus.SUBMIT_SUCCESS
    assert item_submission_instance.ready_to_submit() is False


def test_ready_to_submit_with_max_retries(item_submission_instance):
    item_submission_instance.status = ItemSubmissionStatus.MAX_RETRIES_REACHED
    assert item_submission_instance.ready_to_submit() is False


def test_ready_to_submit_with_reconcile_success(item_submission_instance):
    item_submission_instance.status = ItemSubmissionStatus.RECONCILE_SUCCESS
    assert item_submission_instance.ready_to_submit() is True


def test_ready_to_submit_with_submit_failed(item_submission_instance):
    item_submission_instance.status = ItemSubmissionStatus.SUBMIT_FAILED
    assert item_submission_instance.ready_to_submit() is True


def test_ready_to_submit_with_ingest_failed(item_submission_instance):
    item_submission_instance.status = ItemSubmissionStatus.INGEST_FAILED
    assert item_submission_instance.ready_to_submit() is True
