from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, final

import jsonschema
import jsonschema.exceptions
from botocore.exceptions import ClientError
from pynamodb.exceptions import DoesNotExist

from dsc.config import Config
from dsc.db.models import ItemSubmissionDB, ItemSubmissionStatus
from dsc.exceptions import (
    InvalidDSpaceMetadataError,
    InvalidSQSMessageError,
    InvalidWorkflowNameError,
    ItemMetadatMissingRequiredFieldError,
)
from dsc.item_submission import ItemSubmission
from dsc.utilities.aws import SESClient, SQSClient
from dsc.utilities.validate.schemas import RESULT_MESSAGE_ATTRIBUTES, RESULT_MESSAGE_BODY

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Iterator

    from dsc.reports import Report

logger = logging.getLogger(__name__)
CONFIG = Config()

ITEM_SUBMISSION_LOG_STR = (
    "with primary keys batch_id={batch_id} (hash key) and "
    "item_identifier={item_identifier} (range key)"
)


ITEM_SUBMISSION_LOG_STR = (
    "with primary keys batch_id='{batch_id}' (hash key) and "
    "item_identifier='{item_identifier}' (range key)"
)


@dataclass
class WorkflowEvents:
    """Record of events during the execution of Workflow methods.

    This dataclass is designed to hold useful data used in reporting.
    It is comprised of three lists, which contain details
    about reconciled, submitted, and processed items -- aligning with
    the DSC CLI commands (reconcile, submit, and finalize). Error
    messages are also tracked in a list.
    """

    reconciled_items: dict = field(default_factory=dict)
    submitted_items: list[dict] = field(default_factory=list)
    processed_items: list[dict] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    reconcile_errors: dict = field(default_factory=dict)


class Workflow(ABC):
    """A base workflow class from which other workflow classes are derived."""

    workflow_name: str = "base"
    submission_system: str = "DSpace@MIT"

    def __init__(self, batch_id: str) -> None:
        """Initialize base instance.

        Args:
            batch_id: Unique identifier for a 'batch' deposit that corresponds
                to the name of a subfolder in the workflow directory of the S3 bucket.
                This subfolder is where the S3 client will search for bitstream
                and metadata files.
        """
        self.batch_id = batch_id
        self.workflow_events = WorkflowEvents()
        self.run_date = datetime.now(UTC)
        self.exclude_prefixes: list[str] = ["archived/", "dspace_metadata/"]

    @property
    @abstractmethod
    def metadata_mapping_path(self) -> str:
        """Path to the JSON metadata mapping file for the workflow."""

    @property
    def metadata_mapping(self) -> dict:
        with open(self.metadata_mapping_path) as mapping_file:
            return json.load(mapping_file)

    @final
    @property
    def s3_bucket(self) -> str:
        return CONFIG.s3_bucket_submission_assets

    @property
    def output_queue(self) -> str:
        """The SQS output queue for the DSS result messages."""
        return f"dss-output-dsc-{CONFIG.workspace}"

    @property
    def batch_path(self) -> str:
        return f"{self.workflow_name}/{self.batch_id}/"

    @property
    def retry_threshold(self) -> int:
        return CONFIG.retry_threshold

    @final
    @classmethod
    def get_workflow(cls, workflow_name: str) -> type[Workflow]:
        """Return workflow class.

        Args:
            workflow_name: The label of the workflow. Must match a workflow_name attribute
            from Workflow subclass.
        """
        for workflow_class in cls._get_subclasses():
            if workflow_name == workflow_class.workflow_name:
                return workflow_class
        raise InvalidWorkflowNameError(f"Invalid workflow name: {workflow_name} ")

    @classmethod
    def _get_subclasses(cls) -> Iterator[type[Workflow]]:
        for subclass in cls.__subclasses__():
            yield from subclass._get_subclasses()  # noqa: SLF001
            yield subclass

    @final
    def reconcile_items(self) -> bool:
        """Reconcile item submissions for a batch.

        This method will first reconcile all bitstreams and metadata.
        Next, it will examine the result of the reconcile for each
        item in the batch, using the item identifiers retrieved
        from the metadata*. Results are written to DynamoDB.

        *This may not be the full set of item submissions in a batch
        as there may be bitstreams (intended for item submissions)
        for which an item identifier cannot be retrieved.
        """
        reconciled = self.reconcile_bitstreams_and_metadata()

        # iterate over the results of the reconcile
        # for all item submissions from the metadata
        for item_metadata in self.item_metadata_iter():
            item_identifier = item_metadata["item_identifier"]

            if item_identifier in self.workflow_events.reconciled_items:
                logger.debug(
                    f"Item submission (item_identifier={item_identifier}) reconciled"
                )
                status = ItemSubmissionStatus.RECONCILE_SUCCESS
            else:
                logger.debug(
                    f"Item submission (item_identifier={item_identifier}) "
                    "failed reconcile"
                )
                status = ItemSubmissionStatus.RECONCILE_FAILED

            # create/update record in DynamoDB
            try:
                self.write_reconcile_results_to_dynamodb(item_identifier, status)
            except Exception as exception:  # noqa: BLE001
                logger.error(exception)  # noqa: TRY400
                continue
        return reconciled

    @abstractmethod
    def reconcile_bitstreams_and_metadata(self) -> bool:
        """Reconcile bitstreams against metadata.

        Items in DSpace represent a "work" and combine metadata and files,
        known as "bitstreams". For any given workflow, this method ensures
        the existence of both bitstreams and metadata for each item in the
        batch, verifying that all provided bitstreams can be linked to a
        metadata record and vice versa.

        While this method is not needed for every workflow,
        it MUST be overridden by all workflow subclasses.
        If the workflow does not require this method, the override must
        raise the following exception:

        TypeError(
            f"Method '{self.reconcile_bitstreams_and_metadata.__name__}' "
            f"not used by workflow '{self.__class__.__name__}'."
        )
        """

    @final
    def write_reconcile_results_to_dynamodb(
        self, item_identifier: str, status: str
    ) -> None:
        """Write reconcile results for each item submission to DynamoDB.

        An item submission is first recorded in the DynamoDB table during
        the 'reconcile' step. Once a record is successfully created or
        retrieved from the table, this method will perform a series of
        checks to determine how/what to update for the item submission
        record. If the current status indicates that the item submission
        is at a step that comes after 'reconcile', the record is not
        updated in DynamoDB.
        """
        item_submission_record = ItemSubmissionDB.get_or_create(
            item_identifier=item_identifier,
            batch_id=self.batch_id,
            workflow_name=self.workflow_name,
        )

        if item_submission_record.status in [
            None,
            ItemSubmissionStatus.RECONCILE_FAILED,
        ]:
            logger.info(
                f"Updating record {ITEM_SUBMISSION_LOG_STR.format(batch_id=self.batch_id,
                item_identifier=item_submission_record.item_identifier
                )}"
            )

            item_submission_record.update(
                actions=[
                    ItemSubmissionDB.status.set(status),
                    ItemSubmissionDB.last_run_date.set(self.run_date),
                ]
            )
        elif item_submission_record.status == ItemSubmissionStatus.RECONCILE_SUCCESS:
            logger.debug(
                f"Record "
                f"{ITEM_SUBMISSION_LOG_STR.format(batch_id=self.batch_id,
                                      item_identifier=item_submission_record.item_identifier)
                    } "
                "was previously reconciled, skipping update"
            )
        else:
            logger.info(
                f"Record "
                f"{ITEM_SUBMISSION_LOG_STR.format(batch_id=self.batch_id,
                                      item_identifier=item_submission_record.item_identifier)
                    } "
                "not eligible for reconcile, skipping update"
            )

    @final
    def submit_items(self, collection_handle: str) -> list:
        """Submit items to the DSpace Submission Service according to the workflow class.

        Args:
            collection_handle: The handle of the DSpace collection to which the batch will
              be submitted.


        Returns a dict with the submission results organized into succeeded and failed
        items.
        """
        logger.info(
            f"Submitting messages to the DSS input queue '{CONFIG.sqs_queue_dss_input}' "
            f"for batch '{self.batch_id}'"
        )
        submission_summary = {
            "total": 0,
            "submitted": 0,
            "skipped": 0,
            "errors": 0,
        }

        items = []
        for item_submission in self.item_submissions_iter():
            submission_summary["total"] += 1
            item_identifier = item_submission.item_identifier

            # retrieve record from DynamoDB
            try:
                item_submission_record = ItemSubmissionDB.get(
                    hash_key=self.batch_id, range_key=item_identifier
                )
            except DoesNotExist:
                logger.warning(
                    "Record "
                    f"{ITEM_SUBMISSION_LOG_STR.format(batch_id=self.batch_id,
                                      item_identifier=item_identifier)}"
                    "not found. Verify that it been reconciled."
                )
                submission_summary["skipped"] += 1
                continue

            # validate whether a message should be sent for this item submission
            if not self.allow_submission(item_submission_record):
                submission_summary["skipped"] += 1
                continue

            # upload DSpace metadata file to S3
            try:
                item_submission.upload_dspace_metadata(
                    bucket=self.s3_bucket, prefix=self.batch_path
                )
            except Exception as exception:  # noqa: BLE001
                logger.error(  # noqa: TRY400
                    f"Failed to upload DSpace metadata for item. {exception}"
                )
                self.workflow_events.errors.append(str(exception))
                submission_summary["errors"] += 1

                # update record in DynamoDB
                item_submission_record.update(
                    actions=[
                        ItemSubmissionDB.status.set(ItemSubmissionStatus.SUBMIT_FAILED),
                        ItemSubmissionDB.status_details.set(str(exception)),
                        ItemSubmissionDB.last_run_date.set(self.run_date),
                        ItemSubmissionDB.submit_attempts.add(1),
                    ]
                )
                continue

            # Send submission message to DSS input queue
            try:
                response = item_submission.send_submission_message(
                    self.workflow_name,
                    self.output_queue,
                    self.submission_system,
                    collection_handle,
                )
            except ClientError as exception:
                logger.error(  # noqa: TRY400
                    f"Failed to send submission message for item: {item_identifier}. "
                    f"{exception}"
                )
                self.workflow_events.errors.append(str(exception))
                submission_summary["errors"] += 1

                # update record in DynamoDB
                item_submission_record.update(
                    actions=[
                        ItemSubmissionDB.status.set(ItemSubmissionStatus.SUBMIT_FAILED),
                        ItemSubmissionDB.status_details.set(str(exception)),
                        ItemSubmissionDB.last_run_date.set(self.run_date),
                        ItemSubmissionDB.submit_attempts.add(1),
                    ]
                )
                continue
            except Exception as exception:  # noqa: BLE001
                logger.error(  # noqa: TRY400
                    f"Unexpected error occurred while sending submission message "
                    f"for item: {item_identifier}. {exception}"
                )
                self.workflow_events.errors.append(str(exception))
                submission_summary["errors"] += 1

                # update record in DynamoDB
                item_submission_record.update(
                    actions=[
                        ItemSubmissionDB.status.set(ItemSubmissionStatus.SUBMIT_FAILED),
                        ItemSubmissionDB.status_details.set(str(exception)),
                        ItemSubmissionDB.last_run_date.set(self.run_date),
                        ItemSubmissionDB.submit_attempts.add(1),
                    ]
                )
                continue

            # Record details of the item submission message
            item_data = {
                "item_identifier": item_identifier,
                "message_id": response["MessageId"],
            }
            items.append(item_data)
            self.workflow_events.submitted_items.append(item_data)
            submission_summary["submitted"] += 1

            logger.info(f"Sent item submission message: {item_data["message_id"]}")

            # Set status in DynamoDB
            try:
                item_submission_record.update(
                    actions=[
                        ItemSubmissionDB.status.set(ItemSubmissionStatus.SUBMIT_SUCCESS),
                        ItemSubmissionDB.last_run_date.set(self.run_date),
                        ItemSubmissionDB.submit_attempts.add(1),
                    ]
                )
            except Exception as exception:  # noqa: BLE001
                logger.error(exception)  # noqa: TRY400
                continue

        logger.info(
            f"Submitted messages to the DSS input queue '{CONFIG.sqs_queue_dss_input}' "
            f"for batch '{self.batch_id}': {json.dumps(submission_summary)}"
        )
        return items

    @final
    def item_submissions_iter(self) -> Iterator[ItemSubmission]:
        """Yield item submissions for the DSpace Submission Service.

        MUST NOT be overridden by workflow subclasses.
        """
        for item_metadata in self.item_metadata_iter():
            item_identifier = self.get_item_identifier(item_metadata)
            logger.info(f"Preparing submission for item: {item_identifier}")
            dspace_metadata = self.create_dspace_metadata(item_metadata)
            self.validate_dspace_metadata(dspace_metadata)
            item_submission = ItemSubmission(
                dspace_metadata=dspace_metadata,
                bitstream_s3_uris=self.get_bitstream_s3_uris(item_identifier),
                item_identifier=item_identifier,
            )
            yield item_submission

    @abstractmethod
    def item_metadata_iter(self) -> Iterator[dict[str, Any]]:
        """Iterate through batch metadata to yield item metadata.

        MUST be overridden by workflow subclasses.
        """

    @abstractmethod
    def get_item_identifier(self, item_metadata: dict[str, Any]) -> str:
        """Get identifier for an item submission according to the workflow subclass.

        MUST be overridden by workflow subclasses.

        Args:
            item_metadata: The item metadata from which the item identifier is extracted.
        """

    @final
    def create_dspace_metadata(self, item_metadata: dict[str, Any]) -> dict[str, Any]:
        """Create DSpace metadata from the item's source metadata.

        A metadata mapping is a dict with the format seen below:

        {
            "dc.contributor": {
                "source_field_name": "contributor",
                "language": "<language>",
                "delimiter": "<delimiting character>",
                "required": true | false
            }
        }

        When setting up the metadata mapping JSON file, "language" and "delimiter"
        can be omitted from the file if not applicable. Required fields ("item_identifier"
        and "title") must be set as required (true); if "required" is not listed as a
        a config, the field defaults as not required (false).

        MUST NOT be overridden by workflow subclasses.

        Args:
            item_metadata: Item metadata from which the DSpace metadata will be derived.
        """
        metadata_entries = []
        for field_name, field_mapping in self.metadata_mapping.items():
            if field_name not in ["item_identifier"]:

                field_value = item_metadata.get(field_mapping["source_field_name"])
                if not field_value and field_mapping.get("required", False):
                    raise ItemMetadatMissingRequiredFieldError(
                        "Item metadata missing required field: '"
                        f"{field_mapping["source_field_name"]}'"
                    )

                if field_value:
                    if isinstance(field_value, list):
                        field_values = field_value
                    elif delimiter := field_mapping.get("delimiter"):
                        field_values = field_value.split(delimiter)
                    else:
                        field_values = [field_value]

                    metadata_entries.extend(
                        [
                            {
                                "key": field_name,
                                "value": value,
                                "language": field_mapping.get("language"),
                            }
                            for value in field_values
                        ]
                    )

        return {"metadata": metadata_entries}

    @final
    def validate_dspace_metadata(self, dspace_metadata: dict[str, Any]) -> bool:
        """Validate that DSpace metadata follows the expected format for DSpace 6.x.

        MUST NOT be overridden by workflow subclasses.

        Args:
            dspace_metadata: DSpace metadata to be validated.
        """
        valid = False
        if dspace_metadata.get("metadata") is not None:
            for element in dspace_metadata["metadata"]:
                if element.get("key") is not None and element.get("value") is not None:
                    valid = True
            logger.debug("Valid DSpace metadata created")
        else:
            raise InvalidDSpaceMetadataError(
                f"Invalid DSpace metadata created: {dspace_metadata} ",
            )
        return valid

    @abstractmethod
    def get_bitstream_s3_uris(self, item_identifier: str) -> list[str]:
        """Get bitstreams for an item submission according to the workflow subclass.

        MUST be overridden by workflow subclasses.

        Args:
            item_identifier: The identifier used for locating the item's bitstreams.
        """

    @final
    def allow_submission(self, item_submission_record: ItemSubmissionDB) -> bool:
        """Verify that a submission message should be sent based on status in DynamoDB."""
        allow_submission = False

        match item_submission_record.status:
            case ItemSubmissionStatus.INGEST_SUCCESS:
                logger.info(
                    "Record "
                    f"{ITEM_SUBMISSION_LOG_STR.format(batch_id=self.batch_id,
                                      item_identifier=item_submission_record.item_identifier)
                    } "
                    "already ingested, skipping submission"
                )
            case ItemSubmissionStatus.SUBMIT_SUCCESS:
                logger.info(
                    f"Record "
                    f"{ITEM_SUBMISSION_LOG_STR.format(batch_id=self.batch_id,
                                      item_identifier=item_submission_record.item_identifier)
                    } "
                    " already submitted, skipping submission"
                )
            case ItemSubmissionStatus.MAX_RETRIES_REACHED:
                logger.info(
                    f"Record "
                    f"{ITEM_SUBMISSION_LOG_STR.format(batch_id=self.batch_id,
                                      item_identifier=item_submission_record.item_identifier)
                    } "
                    "max retries reached, skipping submission"
                )
            case None | ItemSubmissionStatus.RECONCILE_FAILED:
                logger.info(
                    f"Record "
                    f"{ITEM_SUBMISSION_LOG_STR.format(batch_id=self.batch_id,
                                      item_identifier=item_submission_record.item_identifier)
                    } "
                    " not reconciled, skipping submission"
                )
            case _:
                logger.debug(
                    f"Record "
                    f"{ITEM_SUBMISSION_LOG_STR.format(batch_id=self.batch_id,
                                      item_identifier=item_submission_record.item_identifier)
                    } "
                    "allowed for submission"
                )
                allow_submission = True

        return allow_submission

    @final
    def finalize_items(self) -> None:
        """Examine results for all item submissions in the batch.

        This method involves three main steps:

        1. Process DSS result messages from the output queue
        2. Apply workflow-specific processing
        3. Load ingest results into WorkflowEvents for reporting

        Must NOT be overridden by workflow subclasses.
        """
        sqs_processed_items = self.process_result_messages()
        self.workflow_specific_processing(sqs_processed_items)

        # update WorkflowEvents with batch-level ingest results
        for item_submission_record in ItemSubmissionDB.query(self.batch_id):
            self.workflow_events.processed_items.append(
                item_submission_record.to_dict(
                    "item_identifier",
                    "status",
                    "status_details",
                    "dspace_handle",
                    "last_result_message",
                )
            )

    def process_result_messages(self) -> list[str]:
        """Process DSS result messages from the output queue.

        This method receives result messages from the output queue, parsing the content
        of each message to determine whether an item was ingested into DSpace.

        The following steps are executed for every result message:

        1. Parse and validate the content of 'MessageAttributes'.
           - If the content is invalid, log an error message, delete
             the message from the output queue, and skip remaining steps.
        2. Get the 'item_identifier' from the result message
           and track ID in list.
        3. Get the record for the item submission from DynamoDB and identify the
           current status.
        4. Parse and validate the content of 'Body'.

           - If the content is invalid, log an error message and track the new status as
             ItemSubmissionStatus.INGEST_UNKNOWN.
           - If the content is valid, check whether item was ingested based on
             'ResultType'.
             - If the item was ingested, get DSpace handle (if available) and track the
               new status as ItemSubmissionStatus.INGEST_SUCCESS.
             - If the item failed ingest, track the new status as
               ItemSubmissionStatus.INGEST_FAILED.
        5. Update the record in DynamoDB with details then delete the message from the
           output queue.

        The method returns a list of item identifiers for item submissions, represented
        by result messages in the output queue.

        May be overridden by workflow subclasses.
        """
        logger.info(
            f"Processing DSS result messages from the output queue '{self.output_queue}'"
        )
        sqs_results_summary = {
            "received_messages": 0,
            "ingest_success": 0,
            "ingest_failed": 0,
            "ingest_unknown": 0,
        }

        sqs_client = SQSClient(
            region=CONFIG.aws_region_name, queue_name=self.output_queue
        )

        sqs_processed_items_ids = []
        for sqs_message in sqs_client.receive():
            sqs_results_summary["received_messages"] += 1

            message_id = sqs_message["MessageId"]
            message_body = sqs_message["Body"]
            receipt_handle = sqs_message["ReceiptHandle"]

            logger.debug(f"Processing result message: {message_id}")

            try:
                message_attributes = self._parse_result_message_attrs(
                    sqs_message["MessageAttributes"]
                )
            except InvalidSQSMessageError as exception:
                logger.error(  # noqa: TRY400
                    f"Failed to parse 'MessageAttributes' from {message_id}: {exception}"
                )
                sqs_results_summary["ingest_unknown"] += 1

                # delete message from the queue
                sqs_client.delete(
                    receipt_handle=receipt_handle,
                    message_id=message_id,
                )
                continue

            item_identifier = message_attributes["PackageID"]["StringValue"]
            sqs_processed_items_ids.append(item_identifier)

            # get record from ItemSubmissionDB
            item_submission_record = ItemSubmissionDB.get(
                hash_key=item_identifier, range_key=self.batch_id
            )
            current_status = item_submission_record.status

            logger.info(
                "Received result message for record "
                f"{ITEM_SUBMISSION_LOG_STR.format(batch_id=item_submission_record.batch_id,
                                      item_identifier=item_submission_record.item_identifier)}"
            )

            try:
                parsed_message_body = self._parse_result_message_body(message_body)
            except InvalidSQSMessageError as exception:
                logger.error(  # noqa: TRY400
                    f"Failed to parse 'Body' from {message_id}:{exception}"
                )
                sqs_results_summary["ingest_unknown"] += 1

                new_status = ItemSubmissionStatus.INGEST_UNKNOWN

                item_submission_record.update(
                    actions=[ItemSubmissionDB.status_details.set(str(exception))]
                )
                logger.info("Unable to determine if ingest status for item.")
            else:
                if bool(parsed_message_body["ResultType"] == "success"):
                    new_status = ItemSubmissionStatus.INGEST_SUCCESS
                    sqs_results_summary["ingest_success"] += 1

                    if dspace_handle := parsed_message_body.get("ItemHandle"):
                        item_submission_record.update(
                            actions=[
                                ItemSubmissionDB.dspace_handle.set(dspace_handle),
                            ]
                        )
                    logger.info("Item was ingested.")
                else:
                    new_status = ItemSubmissionStatus.INGEST_FAILED
                    sqs_results_summary["ingest_failed"] += 1
                    logger.info("Item failed ingest.")

            item_submission_record.update(
                actions=[
                    ItemSubmissionDB.status.set(new_status),
                    ItemSubmissionDB.last_result_message.set(message_body),
                    ItemSubmissionDB.last_run_date.set(self.run_date),
                    ItemSubmissionDB.ingest_attempts.add(1),
                ]
            )

            logger.info(
                "Updated record "
                f"{ITEM_SUBMISSION_LOG_STR.format(batch_id=item_submission_record.batch_id,
                                      item_identifier=item_submission_record.item_identifier)}"
            )
            logger.info(f"Status updated: '{current_status}' -> '{new_status}'")

            # delete message from the queue
            sqs_client.delete(
                receipt_handle=receipt_handle,
                message_id=message_id,
            )

        logger.info(
            f"Processed DSS result messages from the output queue '{self.output_queue}': "
            f"{json.dumps(sqs_results_summary)}"
        )
        return sqs_processed_items_ids

    @final
    @staticmethod
    def _parse_result_message_attrs(message_attributes: dict) -> dict:
        """Parse and validate content of 'MessageAttributes' in result message.

        If the content passes schema validation, the content is returned.

        Raises:
            InvalidSQSMessageError
        """
        # validate content of 'MessageAttributes'
        try:
            jsonschema.validate(
                instance=message_attributes,
                schema=RESULT_MESSAGE_ATTRIBUTES,
            )
        except jsonschema.exceptions.ValidationError as exception:
            raise InvalidSQSMessageError(
                "Content of 'MessageAttributes' failed schema validation"
            ) from exception
        return message_attributes

    @final
    @staticmethod
    def _parse_result_message_body(message_body: str) -> dict:
        """Parse and validate content of 'Body' in result message.

        If the JSON string can be deserialized to a Python dictionary
        and it passes schema validation, the parsed content is returned.

        Raises:
            InvalidSQSMessageError
        """
        # validate content of 'Body'
        try:
            parsed_message_body = json.loads(message_body)
            jsonschema.validate(instance=parsed_message_body, schema=RESULT_MESSAGE_BODY)
        except json.JSONDecodeError as exception:
            raise InvalidSQSMessageError(
                "Failed to parse content of 'Body'"
            ) from exception
        except jsonschema.exceptions.ValidationError as exception:
            raise InvalidSQSMessageError(
                "Content of 'Body' failed schema validation"
            ) from exception
        return parsed_message_body

    def workflow_specific_processing(self, item_identifiers: list[str]) -> None:
        logger.info(
            f"No extra processing for {len(item_identifiers)} items based on workflow: "
            f"'{self.workflow_name}' "
        )

    def send_report(
        self, report_class: type[Report], email_recipients: list[str]
    ) -> None:
        """Send report as an email via SES."""
        report = report_class.from_workflow(self)
        logger.info(f"Sending report to recipients: {email_recipients}")
        ses_client = SESClient(region=CONFIG.aws_region_name)
        ses_client.create_and_send_email(
            subject=report.subject,
            source_email_address=CONFIG.source_email,
            recipient_email_addresses=email_recipients,
            message_body_plain_text=report.to_plain_text(),
            message_body_html=report.to_html(),
            attachments=report.create_attachments(),
        )
