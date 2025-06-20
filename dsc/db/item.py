from typing import TypedDict, Unpack

from pynamodb.attributes import (
    JSONAttribute,
    NumberAttribute,
    UnicodeAttribute,
    UTCDateTimeAttribute,
)
from pynamodb.exceptions import PutError
from pynamodb.models import Model


class OptionalItemAttributes(TypedDict, total=False):
    dspace_handle: str
    status: str
    status_details: str
    ingest_date: str
    last_submission_message: str
    last_result_message: str
    last_run_date: str
    submit_attempts: int
    ingest_attempts: int


class ItemDB(Model):
    """An item submission for DSpace."""

    class Meta:  # noqa: D106
        table_name = "dsc"

    item_identifier = UnicodeAttribute(hash_key=True)
    batch_id = UnicodeAttribute(range_key=True)
    workflow_name = UnicodeAttribute()
    dspace_handle = UnicodeAttribute(null=True)
    status = UnicodeAttribute(null=True)
    status_details = UnicodeAttribute(null=True)
    ingest_date = UTCDateTimeAttribute(null=True)
    last_submission_message = JSONAttribute(null=True)
    last_result_message = JSONAttribute(null=True)
    last_run_date = UTCDateTimeAttribute(null=True)
    submit_attempts = NumberAttribute(default_for_new=0)
    ingest_attempts = NumberAttribute(default_for_new=0)

    @classmethod
    def set_table_name(cls, table_name: str) -> None:
        """Set table_name attribute.

        The table name must be set dynamically rather than from an env variable
        due to the current configuration process.

        Args:
            table_name: The name of the DynamoDB table.
        """
        cls.Meta.table_name = table_name

    @classmethod
    def create(
        cls,
        item_identifier: str,
        batch_id: str,
        workflow_name: str,
        **attributes: Unpack[OptionalItemAttributes],
    ) -> None:
        item = cls(
            item_identifier=item_identifier,
            batch_id=batch_id,
            workflow_name=workflow_name,
            **attributes,
        )

        try:
            item.save(
                condition=cls.item_identifier.does_not_exist()
                & cls.batch_id.does_not_exist()
            )
        except PutError as exception:
            raise ValueError(
                f"Item with item_identifier={item_identifier} (hash key) and "
                f"batch_id={batch_id} (range_key) already exists"
            ) from exception
