import logging
from enum import StrEnum
from typing import TypedDict

from pynamodb.attributes import (
    JSONAttribute,
    NumberAttribute,
    UnicodeAttribute,
    UTCDateTimeAttribute,
)
from pynamodb.exceptions import PutError
from pynamodb.models import Model

from dsc.exceptions import ItemSubmissionCreateError, ItemSubmissionExistsError

logger = logging.getLogger(__name__)
ITEM_SUBMISSION_LOG_STR = (
    "with primary keys batch_id={batch_id} (hash key) and "
    "item_identifier={item_identifier} (range key)"
)


class ItemSubmissionStatus(StrEnum):
    BATCH_CREATED = "batch_created"
    RECONCILE_SUCCESS = "reconcile_success"
    RECONCILE_FAILED = "reconcile_failed"
    SUBMIT_SUCCESS = "submit_success"
    SUBMIT_FAILED = "submit_failed"
    INGEST_SUCCESS = "ingest_success"
    INGEST_FAILED = "ingest_failed"
    INGEST_UNKNOWN = "ingest_unknown"
    MAX_RETRIES_REACHED = "max_retries_reached"


class OptionalItemAttributes(TypedDict, total=False):
    source_system_identifier: str
    collection_handle: str
    dspace_handle: str
    status: str
    status_details: str
    ingest_date: str
    last_result_message: str
    last_run_date: str
    submit_attempts: int
    ingest_attempts: int


class ItemSubmissionDB(Model):
    """A DynamoDB model representing an item submission.

    This model stores information about the state of an item submission
    as it progresses through the DSC workflow. The table uses a
    composite primary key consisting of 'batch_id' (partition key)
    and item_identifier (sort key).

    Attributes:
        batch_id [partition key]: A unique identifier for a workflow run,
            also used as an S3 prefix for a workflow run's submission assets.
        item_identifier [sort key]: A unique identifier for an item submission
            in a batch.
        workflow_name: The name of the DSC workflow.
        source_system_identifier: An identifier used to linked the ingested item to its
        record in a source system.
        collection_handle: A persistent, globally unique identifier for the
            collection in DSpace that the item will include in the DSS message.
        dspace_handle: A persistent, globally unique identifier for the item
            in DSpace. The handle is provided in the DSS result message when
            an item is successfully ingested into DSpace.
            NOTE: If the item is sent to a DSpace submission queue, the handle is
            NOT provided.
        status: The current state of an item submission in the DSC workflow.
            See dsc.db.models.ItemSubmissionStatus for accepted values.
        status_details: Additional details regarding the status of an item
            (e.g., error messages).
        ingest_date: A date representing when an item was successfully ingested
            into DSpace. In DynamoDB, the date is stored as a string
            (in ISO 8601 format).
        last_result_message: A serialized JSON string of the latest (most recent)
            result message composed and sent to the output SQS queue for DSC via DSS.
        last_run_date: A date representing the last time a DSC CLI command was executed
            on the item. In DynamoDB, the date is stored as a string (in ISO 8601 format).
        submit_attempts: The number of attempts to send a submission message to the
            input SQS queue for DSC. This value is only incremented when the DSC
            submit command is run for an item.
        ingest_attempts: The number of attempts to ingest an item into DSpace via DSS.
            This value is only incremented when the DSC finalize command is run for
            an item.
    """

    class Meta:  # noqa: D106
        table_name = "dsc"

    batch_id = UnicodeAttribute(hash_key=True)
    item_identifier = UnicodeAttribute(range_key=True)
    workflow_name = UnicodeAttribute()
    source_system_identifier = UnicodeAttribute(null=True)
    collection_handle = UnicodeAttribute(null=True)
    dspace_handle = UnicodeAttribute(null=True)
    status = UnicodeAttribute(null=True)
    status_details = UnicodeAttribute(null=True)
    ingest_date = UTCDateTimeAttribute(null=True)
    last_result_message = JSONAttribute(null=True)
    last_run_date = UTCDateTimeAttribute(null=True)
    submit_attempts = NumberAttribute(default_for_new=0)
    ingest_attempts = NumberAttribute(default_for_new=0)

    @classmethod
    def set_table_name(cls, table_name: str) -> None:
        """Set Meta.table_name attribute.

        The table name must be set dynamically rather than from an env variable
        due to the current configuration process.

        Args:
            table_name: The name of the DynamoDB table.
        """
        cls.Meta.table_name = table_name

    def create(self) -> "ItemSubmissionDB":
        """Create a new item (row) in the DynamoDB table.

        This method attempts to save the current instance to the DynamoDB
        table, enforcing a condition that prevents overwriting an existing
        item with the same primary keys.

        If the call to the save method fails for any reason, a
        pynamodb.exceptions.PutError is raised.

            - If the error is caused by the failing condition,
              raise ItemSubmissionExistsError
            - Otherwise, raise ItemSubmissioNCreate error with the
              message describing the cause.

        Raises:
            ItemSubmissionCreateError
            ItemSubmissionExistsError
        """
        try:
            self.save(
                condition=ItemSubmissionDB.item_identifier.does_not_exist()
                & ItemSubmissionDB.batch_id.does_not_exist()
            )
            logger.info(
                "Created record "
                f"{ITEM_SUBMISSION_LOG_STR.format(batch_id=self.batch_id,
                                      item_identifier=self.item_identifier)}"
            )
        except PutError as exception:
            # if the `PutError` is due to failing conditional check, this means
            # a row for the item submission already exists in DynamoDB
            if exception.cause_response_code == "ConditionalCheckFailedException":
                raise ItemSubmissionExistsError(
                    f"Item with batch={self.batch_id} (hash key) and "
                    f"item_identifier={self.item_identifier} (range key) already exists"
                ) from exception

            # if the `PutError` is due to any other cause,
            # note the cause in a custom 'catch-all' exception for put errors
            raise ItemSubmissionCreateError(
                exception.cause_response_message
            ) from exception

        return self

    def to_dict(self, *attributes: str) -> dict:
        """Create dict representing an item submission.

        This method will create a dict where the order of the keys
        matches the order of the attributes passed in as positional args.

        Args:
            *attributes: Names of the model's attributes passed in as
                positional args.
        """
        simple_dict = self.to_simple_dict()
        return {attr: simple_dict.get(attr) for attr in attributes}
