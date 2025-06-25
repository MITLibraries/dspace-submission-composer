import pytest

from dsc.db.exceptions import ItemSubmissionExistsError
from dsc.db.models import ItemSubmissionDB


def test_db_itemsubmission_create_success(mocked_item_db):
    ItemSubmissionDB.create(
        batch_id="batch-aaa", item_identifier="123", workflow_name="workflow"
    )

    # retrieve created 'item' from ItemDB
    fetched_item = ItemSubmissionDB.get(hash_key="batch-aaa", range_key="123")

    assert fetched_item.item_identifier == "123"
    assert fetched_item.batch_id == "batch-aaa"
    assert fetched_item.workflow_name == "workflow"


def test_db_itemsubmission_create_if_exists_raise_error(mocked_item_db):
    ItemSubmissionDB.create(
        batch_id="batch-aaa", item_identifier="123", workflow_name="workflow"
    )

    with pytest.raises(ItemSubmissionExistsError):
        ItemSubmissionDB.create(
            batch_id="batch-aaa", item_identifier="123", workflow_name="workflow"
        )
