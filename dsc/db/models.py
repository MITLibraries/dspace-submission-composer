from enum import StrEnum
from typing import TypedDict, Unpack

from pynamodb.attributes import (
    JSONAttribute,
    NumberAttribute,
    UnicodeAttribute,
    UTCDateTimeAttribute,
)
from pynamodb.exceptions import PutError
from pynamodb.models import Model

from dsc.db.exceptions import ItemSubmissionExistsError


class ItemSubmissionStatus(StrEnum):
    RECONCILE_SUCCESS = "reconcile_success"
    RECONCILE_FAILED = "reconcile_failed"
    SUBMIT_SUCCESS = "submit_success"
    SUBMIT_FAILED = "submit_failed"
    SUBMIT_MAX_RETRIES_REACHED = "submit_max_retries_reached"
    INGEST_SUCCESS = "ingest_success"
    INGEST_FAILED = "ingest_failed"
    INGEST_UNKNOWN = "ingest_unknown"
    INGEST_MAX_RETRIES_REACHED = "ingest_max_retries_reached"


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


class ItemSubmissionDB(Model):
    """A DynamoDB model representing an item submission.

    This model stores information about the state of an item submission
    as it progresses through the DSC workflow. The table uses a
    composite primary key consisting of 'batch_id' (partition key)
    and item_identifier (sort key).

    Attributes:
        batch_id [partition key]: A unique identifier for the workflow run,
            also used as an S3 prefix for workflow run files.
        item_identifier [sort key]: A unique identifier for an item submission
            in a batch.
        workflow_name: The name of the DSC workflow.
        dspace_handle: A persistent, globally unique identifier for a digital object
            in DSpace. The handle is provided in the DSS result message when
            an item is successfully ingested into DSpace.
            NOTE: If the item is sent to a submission queue, the handle is
            NOT provided.
        status: The current state of an item submission in the DSC workflow.
            See dsc.db.models.ItemSubmissionStatus for accepted values.
        status_details: Additional details regarding the status of an item
            (e.g., error messages).
        ingest_date: A date representing when an item was successfully ingested
            into DSpace. In DynamoDB, the date is stored as a string
            (in ISO 8601 format).
        last_submission_message: A serialized JSON string of the latest (most recent)
            submission message composed and sent to the input SQS queue via
            the submit command.
        last_result_message: A serialized JSON string of the latest (most recent)
            result message composed and sent to the output SQS queue for DSC via DSS.
        last_run_date: A date representing the last time a DSC CLI command was executed
            on the item. In DynamoDB, the date is stored as a string (in ISO 8601 format).
        submit_attempts: The number of attempts to send a submission message to the
            input SQS queue for DSC. This value is only incremented when the DSC
            submit command is run for an item.
        ingest_attempts: The number of attempts to ingest an item into DSpace (run DSS).
            This value is only incremented when the DSC finalize command is run for
            an item.
    """

    class Meta:  # noqa: D106
        table_name = "dsc"

    batch_id = UnicodeAttribute(hash_key=True)
    item_identifier = UnicodeAttribute(range_key=True)
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
        """Set Meta.table_name attribute.

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
        """Create a new item (row) in the 'dsc-item-submissions' table.

        This method also calls self.save() to write the item to DynamoDB.
        A condition is included in the 'save' call to prevent overwriting
        entries in the table that have the same primary keys.

        If the call to the save method fails due to the set condition, a
        db.exceptions.X is raised; otherwise, it re-raises
        the pynamodb.exceptions.PutError.
        """
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
            if exception.cause_response_code == "ConditionalCheckFailedException":
                raise ItemSubmissionExistsError(
                    f"Item with item_identifier={item_identifier} (hash key) and "
                    f"batch_id={batch_id} (range_key) already exists"
                ) from exception
            raise
