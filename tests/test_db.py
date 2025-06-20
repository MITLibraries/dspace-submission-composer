import pytest

from dsc.db.item import ItemDB


def test_db_item_create_success(mocked_item_db):
    ItemDB.create(item_identifier="123", batch_id="batch-aaa", workflow_name="workflow")

    # retrieve created 'item' from ItemDB
    fetched_item = ItemDB.get(hash_key="123", range_key="batch-aaa")

    assert fetched_item.item_identifier == "123"
    assert fetched_item.batch_id == "batch-aaa"
    assert fetched_item.workflow_name == "workflow"


def test_db_item_create_if_hash_key_and_range_key_exist_raise_error(mocked_item_db):
    ItemDB.create(item_identifier="123", batch_id="batch-aaa", workflow_name="workflow")

    with pytest.raises(ValueError):  # noqa: PT011
        ItemDB.create(
            item_identifier="123", batch_id="batch-aaa", workflow_name="workflow"
        )
