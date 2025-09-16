from datetime import UTC, datetime, timedelta

import pytest
import smart_open
from freezegun import freeze_time

from dsc.db.models import ItemSubmissionDB, ItemSubmissionStatus


@freeze_time("2025-01-01 09:00:00")
def test_workflow_specific_processing_success(
    mocked_item_submission_db,
    archivesspace_workflow_instance,
    mocked_s3,
    s3_client,
    caplog,
):
    caplog.set_level("DEBUG")

    run_date = datetime.now(UTC)
    run_date_str = run_date.strftime("%Y-%m-%d-%H:%M:%S")

    ItemSubmissionDB(
        item_identifier="123",
        batch_id="batch-aaa",
        workflow_name="archivesspace",
        dspace_handle="handle/123",
        source_system_identifier="archives/456",
        status=ItemSubmissionStatus.INGEST_SUCCESS,
        last_run_date=run_date,
    ).create()
    s3_client.client.create_bucket(Bucket="output-bucket")

    archivesspace_workflow_instance.workflow_specific_processing()

    with smart_open.open(
        f"s3://output-bucket/{archivesspace_workflow_instance.batch_id}-{run_date_str}.csv"
    ) as csv_file:
        assert csv_file.read() == "ao_uri,dspace_handle\narchives/456,handle/123\n"
    assert (
        f"Completed ingest report for batch '{archivesspace_workflow_instance.batch_id}' "
        f"on run date '{run_date_str}'" in caplog.text
    )


@freeze_time("2025-01-01 09:00:00")
def test_workflow_specific_processing_wrong_status_skipped(
    archivesspace_workflow_instance,
    mocked_item_submission_db,
    mocked_s3,
    s3_client,
    caplog,
):
    caplog.set_level("DEBUG")

    run_date = datetime.now(UTC)
    run_date_str = run_date.strftime("%Y-%m-%d-%H:%M:%S")

    ItemSubmissionDB(
        item_identifier="123",
        batch_id="batch-aaa",
        workflow_name="archivesspace",
        dspace_handle="handle/123",
        source_system_identifier="archives/456",
        status=ItemSubmissionStatus.RECONCILE_SUCCESS,
        last_run_date=run_date,
    ).create()
    s3_client.client.create_bucket(Bucket="output-bucket")

    archivesspace_workflow_instance.workflow_specific_processing()

    with pytest.raises(OSError, match="The specified key does not exist"):
        smart_open.open(
            f"s3://output-bucket/{archivesspace_workflow_instance.batch_id}-{run_date_str}.csv"
        )
    assert (
        f"No items ingested for '{archivesspace_workflow_instance.batch_id}' on run "
        f"date '{run_date_str}'" in caplog.text
    )


@freeze_time("2025-01-01 09:00:00")
def test_workflow_specific_processing_wrong_date_skipped(
    archivesspace_workflow_instance,
    mocked_item_submission_db,
    mocked_s3,
    s3_client,
    caplog,
):
    caplog.set_level("DEBUG")

    run_date = datetime.now(UTC)
    wrong_time = run_date + timedelta(hours=9)
    run_date_str = run_date.strftime("%Y-%m-%d-%H:%M:%S")

    ItemSubmissionDB(
        item_identifier="123",
        batch_id="batch-aaa",
        workflow_name="archivesspace",
        dspace_handle="handle/123",
        source_system_identifier="archives/456",
        status=ItemSubmissionStatus.INGEST_SUCCESS,
        last_run_date=wrong_time,
    ).create()
    s3_client.client.create_bucket(Bucket="output-bucket")

    archivesspace_workflow_instance.workflow_specific_processing()

    with pytest.raises(OSError, match="The specified key does not exist"):
        smart_open.open(
            f"s3://output-bucket/{archivesspace_workflow_instance.batch_id}-{run_date_str}.csv"
        )
    assert (
        f"No items ingested for '{archivesspace_workflow_instance.batch_id}' on run "
        f"date '{run_date_str}'" in caplog.text
    )
