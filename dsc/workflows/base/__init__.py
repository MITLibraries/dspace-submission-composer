from __future__ import annotations

import itertools
import json
import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, final

import jsonschema
import jsonschema.exceptions

from dsc.config import Config
from dsc.db.models import ItemSubmissionDB, ItemSubmissionStatus
from dsc.exceptions import (
    InvalidSQSMessageError,
    InvalidWorkflowNameError,
    ReconcileFoundBitstreamsWithoutMetadataWarning,
    ReconcileFoundMetadataWithoutBitstreamsWarning,
)
from dsc.item_submission import ItemSubmission
from dsc.utilities.aws import SESClient, SQSClient
from dsc.utilities.validate.schemas import RESULT_MESSAGE_ATTRIBUTES, RESULT_MESSAGE_BODY

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Iterator

    from mypy_boto3_sqs.type_defs import MessageTypeDef

    from dsc.reports import Report

logger = logging.getLogger(__name__)
CONFIG = Config()

ITEM_SUBMISSION_LOG_STR = (
    "with primary keys batch_id={batch_id} (hash key) and "
    "item_identifier={item_identifier} (range key)"
)


@dataclass
class DSSResultMessage:
    """Represents a parsed DSpace Submission Service result message."""

    item_identifier: str
    submission_source: str
    result_type: str
    dspace_handle: str | None
    last_modified: str | None
    error_info: str | None
    error_timestamp: str | None
    dspace_response: str | None
    exception_traceback: list[str] | None
    message_id: str
    receipt_handle: str
    raw_message: MessageTypeDef

    @classmethod
    def from_result_message(cls, message: MessageTypeDef) -> DSSResultMessage:
        """Create instance from result message.

        Args:
            message: A result message from the DSS output queue

        Raises:
            InvalidSQSMessageError: If message fails validation
        """
        try:
            attrs = message.get("MessageAttributes", {})
            jsonschema.validate(instance=attrs, schema=RESULT_MESSAGE_ATTRIBUTES)

            body = json.loads(message.get("Body", "{}"))
            jsonschema.validate(instance=body, schema=RESULT_MESSAGE_BODY)

            return cls(
                item_identifier=attrs["PackageID"]["StringValue"],
                submission_source=attrs["SubmissionSource"]["StringValue"],
                result_type=body["ResultType"],
                dspace_handle=body.get("ItemHandle"),
                last_modified=body.get("lastModified"),
                error_info=body.get("ErrorInfo", "Unknown error"),
                error_timestamp=body.get("ErrorTimestamp"),
                dspace_response=body.get("DSpaceResponse"),
                exception_traceback=body.get("ExceptionTraceback"),
                message_id=message["MessageId"],
                receipt_handle=message["ReceiptHandle"],
                raw_message=message,
            )

        except (KeyError, json.JSONDecodeError) as exception:
            raise InvalidSQSMessageError(
                f"Failed to parse result message: {exception}"
            ) from exception
        except jsonschema.exceptions.ValidationError as exception:
            raise InvalidSQSMessageError(
                f"Result message failed schema validation: {exception}"
            ) from exception


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

        # cache list of bitstream uris
        self.batch_bitstreams = self.get_batch_bitstreams()

        # capture high-level results of executed commands
        self.submission_summary: dict[str, int] = {
            "total": 0,
            "submitted": 0,
            "skipped": 0,
            "errors": 0,
        }

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

    @abstractmethod
    def get_batch_bitstreams(self) -> list[str]:
        """Get all bitstreams for a batch"""

    @final
    def get_item_bitstreams(self, item_identifier: str) -> list[str]:
        return [
            bitstream
            for bitstream in self.batch_bitstreams
            if item_identifier in bitstream
        ]

    @abstractmethod
    def item_metadata_iter(self) -> Iterator[dict[str, Any]]:
        """Iterate through batch metadata to yield item metadata.

        MUST be overridden by workflow subclasses.
        """

    def reconcile_items(self):
        reconciled = {}  # dict where keys = item_identifier, values = bitstream uris
        bitstreams_without_metadata = []
        metadata_without_bitstreams = []

        # loop through each item metadata
        for item_metadata in self.item_metadata_iter():
            item_submission = ItemSubmission.get_or_create(
                batch_id=self.batch_id,
                item_identifier=item_metadata["item_identifier"],
                workflow_name=self.workflow_name,
            )

            # attach source metadata
            item_submission.source_metadata = item_metadata

            # get current reconcile status and status details
            reconcile_status, status_details = self.reconcile_item(item_submission)
            if reconcile_status:
                status = ItemSubmissionStatus.RECONCILE_SUCCESS
                reconciled[item_submission.item_identifier] = self.get_item_bitstreams(
                    item_submission.item_identifier
                )
            else:
                status = ItemSubmissionStatus.RECONCILE_FAILED
                if status_details == "missing bitstreams":
                    status = ItemSubmissionStatus.RECONCILE_FAILED
                    metadata_without_bitstreams.append(item_submission.item_identifier)

            # if the item was previously 'reconciled', skipping table updates
            if item_submission.status not in [
                None,
                ItemSubmissionStatus.RECONCILE_FAILED,
            ]:
                continue

            # update item submission in table
            item_submission.last_run_date = self.run_date
            item_submission.status = status
            item_submission.status_details = status_details
            item_submission.upsert_db()

            logger.debug(
                "Updated status for the item submission(item_identifier="
                f"{item_submission.item_identifier}): {item_submission.status}"
            )

        # check for unmatched bitstreams
        bitstreams_without_metadata.extend(
            list(set(self.batch_bitstreams) - set(itertools.chain(*reconciled.values())))
        )

        reconcile_summary = {
            "reconciled": len(reconciled),
            "bitstreams_without_metadata": len(bitstreams_without_metadata),
            "metadata_without_bitstreams": len(metadata_without_bitstreams),
        }
        logger.info(
            f"Ran reconcile for batch '{self.batch_id}': {json.dumps(reconcile_summary)}"
        )

        # TODO: The code below can be simplified once reporting modules are updated
        # so that it no longer rely on WorkflowEvents
        if any((bitstreams_without_metadata, metadata_without_bitstreams)):
            logger.warning("Failed to reconcile bitstreams and metadata")

            if bitstreams_without_metadata:
                logger.warning(
                    ReconcileFoundBitstreamsWithoutMetadataWarning(
                        bitstreams_without_metadata
                    )
                )
                self.workflow_events.reconcile_errors["bitstreams_without_metadata"] = (
                    bitstreams_without_metadata
                )

            if metadata_without_bitstreams:
                logger.warning(
                    ReconcileFoundMetadataWithoutBitstreamsWarning(
                        metadata_without_bitstreams
                    )
                )
                self.workflow_events.reconcile_errors["metadata_without_bitstreams"] = (
                    metadata_without_bitstreams
                )
            return False
        else:
            logger.info(
                "Successfully reconciled bitstreams and metadata for all "
                f"{len(reconciled)} item(s)"
            )
            return True

    @abstractmethod
    def reconcile_item(self, item_submission: ItemSubmission) -> tuple[bool, None | str]:
        """Reconcile item submissions for a batch.

        Items in DSpace represent a "work" and combine metadata and files,
        known as "bitstreams". For any given workflow, this method ensures
        the existence of both bitstreams and metadata for each item in the
        batch, verifying that all provided bitstreams can be linked to a
        metadata record and vice versa.

        For each instance, data will be loaded in from the associated record in DynamoDB.
        The method then conditionally updates the record in the table based on
        its "current status". If the record is not yet recorded in DynamoDB, it will
        first create and save the record to the table (status=None).

        NOTE: If the workflow does not require this method, the override must
        raise the following exception:

        TypeError(
            f"Method '{self.reconcile_items.__name__}' "
            f"not used by workflow '{self.__class__.__name__}'."
        )
        """

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

        batch_metadata = {
            item_metadata["item_identifier"]: item_metadata
            for item_metadata in self.item_metadata_iter()
        }

        items = []
        for item_submission in ItemSubmission.get_batch(self.batch_id):

            # instantiate ItemSubmission instance from DB record
            self.submission_summary["total"] += 1
            item_identifier = item_submission.item_identifier
            logger.debug(f"Preparing submission for item: {item_identifier}")
            item_submission.last_run_date = self.run_date

            # validate whether a message should be sent for this item submission
            if not item_submission.ready_to_submit():
                self.submission_summary["skipped"] += 1
                continue
            try:
                # prepare submission assets
                item_submission.prepare_dspace_metadata(
                    metadata_mapping=self.metadata_mapping,
                    item_metadata=batch_metadata[item_identifier],
                    s3_bucket=self.s3_bucket,
                    batch_path=self.batch_path,
                )
                item_submission.bitstream_s3_uris = self.get_bitstream_s3_uris(
                    item_identifier
                )

                # Send submission message to DSS input queue
                response = item_submission.send_submission_message(
                    self.workflow_name,
                    self.output_queue,
                    self.submission_system,
                    collection_handle,
                )

                # Record details of the item submission message
                item_data = {
                    "item_identifier": item_identifier,
                    "message_id": response["MessageId"],
                }
                items.append(item_data)
                self.workflow_events.submitted_items.append(item_data)
                self.submission_summary["submitted"] += 1

                logger.info(f"Sent item submission message: {item_data["message_id"]}")

                # Set status in DynamoDB
                item_submission.status = ItemSubmissionStatus.SUBMIT_SUCCESS
                item_submission.submit_attempts += 1
                item_submission.upsert_db()
            except Exception as exception:  # noqa: BLE001
                self.workflow_events.errors.append(str(exception))
                self.submission_summary["errors"] += 1

                item_submission.status = ItemSubmissionStatus.SUBMIT_FAILED
                item_submission.status_details = str(exception)
                item_submission.submit_attempts += 1
                item_submission.upsert_db()

        logger.info(
            f"Submitted messages to the DSS input queue '{CONFIG.sqs_queue_dss_input}' "
            f"for batch '{self.batch_id}': {json.dumps(self.submission_summary)}"
        )
        return items

    @abstractmethod
    def get_bitstream_s3_uris(self, item_identifier: str) -> list[str]:
        """Get bitstreams for an item submission according to the workflow subclass.

        MUST be overridden by workflow subclasses.

        Args:
            item_identifier: The identifier used for locating the item's bitstreams.
        """

    @final
    def finalize_items(self) -> None:
        """Examine results for all item submissions in the batch.

        This method involves three main steps:

        1. Process DSS result messages from the output queue
        2. Apply workflow-specific processing
        3. Load ingest results into WorkflowEvents for reporting

        Must NOT be overridden by workflow subclasses.
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

        # retrieve and create map of result messages
        sqs_client = SQSClient(
            region=CONFIG.aws_region_name, queue_name=self.output_queue
        )
        logger.info(
            f"Processing DSS result messages from the output queue '{self.output_queue}'"
        )
        result_message_map: dict[str, DSSResultMessage] = {}
        for message in sqs_client.receive():
            try:
                result_message_object = DSSResultMessage.from_result_message(message)
                result_message_map[result_message_object.item_identifier] = (
                    result_message_object
                )
            except InvalidSQSMessageError:
                logger.exception(f"Failure parsing message '{message}'")
                continue

        sqs_results_summary["received_messages"] = len(result_message_map)

        # retrieve item submissions from batch
        for item_submission in ItemSubmission.get_batch(self.batch_id):
            log_str = ITEM_SUBMISSION_LOG_STR.format(
                batch_id=self.batch_id, item_identifier=item_submission.item_identifier
            )
            if item_submission.status == ItemSubmissionStatus.INGEST_SUCCESS:
                logger.debug(f"Record {log_str} already ingested, skipping")
                continue

            item_submission.ingest_attempts += 1

            result_message = result_message_map.get(item_submission.item_identifier)

            # skip item submission if result message is not found
            if not result_message:
                continue

            # update item submission status based on ingest result
            if result_message.result_type == "success":
                item_submission.status = ItemSubmissionStatus.INGEST_SUCCESS
                item_submission.dspace_handle = result_message.dspace_handle
                sqs_results_summary["ingest_success"] += 1
                logger.debug(f"Record {log_str} was ingested")
            elif result_message.result_type == "error":
                item_submission.status = ItemSubmissionStatus.INGEST_FAILED
                item_submission.status_details = result_message.error_info
                sqs_results_summary["ingest_failed"] += 1
                logger.debug(f"Record {log_str} failed to ingest")
            else:
                item_submission.status = ItemSubmissionStatus.INGEST_UNKNOWN
                sqs_results_summary["ingest_unknown"] += 1
                logger.debug(f"Unable to determine ingest status for record {log_str}")
            item_submission.last_result_message = str(result_message.raw_message)
            item_submission.last_run_date = self.run_date
            item_submission.upsert_db()
            sqs_client.delete(
                receipt_handle=result_message.receipt_handle,
                message_id=result_message.message_id,
            )

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

        # optional method used for some workflows
        self.workflow_specific_processing()

        logger.info(
            f"Processed DSS result messages from the output queue '{self.output_queue}': "
            f"{json.dumps(sqs_results_summary)}"
        )

    def workflow_specific_processing(self) -> None:
        logger.info(
            f"No extra processing for batch based on workflow: "
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
