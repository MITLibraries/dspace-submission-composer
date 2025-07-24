from unittest.mock import patch

import pytest
from pynamodb.exceptions import PutError

from dsc.db.exceptions import ItemSubmissionCreateError, ItemSubmissionExistsError
from dsc.db.models import ItemSubmissionDB


def test_db_itemsubmission_create_success(mocked_item_submission_db):
    ItemSubmissionDB.create(
        batch_id="batch-aaa", item_identifier="123", workflow_name="workflow"
    )

    # retrieve created 'item' from ItemDB
    fetched_item = ItemSubmissionDB.get(hash_key="batch-aaa", range_key="123")

    assert fetched_item.item_identifier == "123"
    assert fetched_item.batch_id == "batch-aaa"
    assert fetched_item.workflow_name == "workflow"


def test_db_itemsubmission_create_if_exists_raise_error(mocked_item_submission_db):
    ItemSubmissionDB.create(
        batch_id="batch-aaa", item_identifier="123", workflow_name="workflow"
    )

    with pytest.raises(ItemSubmissionExistsError):
        ItemSubmissionDB.create(
            batch_id="batch-aaa", item_identifier="123", workflow_name="workflow"
        )


@patch("dsc.db.models.ItemSubmissionDB.save")
def test_db_itemsubmission_create_if_puterror_raise_error(
    mock_item_submission_db_save, mocked_item_submission_db
):

    class MockDynamoDBError(Exception):
        cause_response_message = "This is a mocked DynamoDB Exception"

    mock_item_submission_db_save.side_effect = PutError(cause=MockDynamoDBError)

    with pytest.raises(ItemSubmissionCreateError):
        ItemSubmissionDB.create(
            batch_id="batch-aaa", item_identifier="123", workflow_name="workflow"
        )


def test_db_itemsubmission_get_or_create_success(mocked_item_submission_db):
    ItemSubmissionDB.create(
        batch_id="batch-aaa", item_identifier="123", workflow_name="workflow"
    )

    # retrieve created 'item' from ItemDB
    fetched_item = ItemSubmissionDB.get_or_create(
        batch_id="batch-aaa", item_identifier="123", workflow_name="workflow"
    )

    assert fetched_item
    assert fetched_item.item_identifier == "123"
    assert fetched_item.batch_id == "batch-aaa"
    assert fetched_item.workflow_name == "workflow"


def test_db_itemsubmission_get_batch_items_success(mocked_item_submission_db):
    ItemSubmissionDB.create(
        batch_id="batch-aaa", item_identifier="123", workflow_name="workflow"
    )
    ItemSubmissionDB.create(
        batch_id="batch-bbb", item_identifier="1234", workflow_name="workflow"
    )

    items = ItemSubmissionDB.get_batch_items(batch_id="batch-aaa")

    assert len(items) == 1
    assert items[0].item_identifier == "123"
    assert all(item.batch_id == "batch-aaa" for item in items)
