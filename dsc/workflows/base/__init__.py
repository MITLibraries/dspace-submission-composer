from __future__ import annotations

import itertools
import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import UTC, datetime
from io import StringIO
from typing import TYPE_CHECKING, Any, final

import jsonschema
import jsonschema.exceptions

from dsc.config import Config
from dsc.db.models import ItemSubmissionStatus
from dsc.exceptions import (
    BatchCreationFailedError,
    InvalidSQSMessageError,
    InvalidWorkflowNameError,
    ReconcileFailedError,
    ReconcileFailedMissingBitstreamsError,
    ReconcileFoundBitstreamsWithoutMetadataWarning,
    ReconcileFoundMetadataWithoutBitstreamsWarning,
)
from dsc.item_submission import ItemSubmission
from dsc.reports import Report
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
        self.run_date = datetime.now(UTC)
        self.exclude_prefixes: list[str] = [
            "archived/",
            "dspace_metadata/",
            f"{self.batch_path}metadata.csv",
        ]
        self.submission_summary: dict[str, int] = {
            "total": 0,
            "submitted": 0,
            "skipped": 0,
            "errors": 0,
        }

        # cache list of bitstreams
        self._batch_bitstream_uris: list[str] | None = None

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
    def batch_bitstream_uris(self) -> list[str]:
        if not self._batch_bitstream_uris:
            self._batch_bitstream_uris = self.get_batch_bitstream_uris()
        return self._batch_bitstream_uris

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
    def get_batch_bitstream_uris(self) -> list[str]:
        """Get list of bitstream URIs for a batch."""

    @final
    def get_item_bitstream_uris(self, item_identifier: str) -> list[str]:
        """Get list of bitstreams URIs for an item."""
        return [uri for uri in self.batch_bitstream_uris if item_identifier in uri]

    @abstractmethod
    def item_metadata_iter(self) -> Iterator[dict[str, Any]]:
        """Iterate through batch metadata to yield item metadata.

        MUST be overridden by workflow subclasses.
        """

    @final
    def create_batch(self, *, synced: bool = False) -> None:
        """Create a batch of item submissions for processing.

        A "batch" refers to a collection of item submissions that are grouped together
        for coordinated processing, storage, and workflow execution. Each batch
        typically consists of multiple items, each with its own metadata and
        associated files, organized under a unique batch identifier.

        This method prepares the necessary assets in S3 (programmatically as needed)
        and records each item in the batch to DynamoDB.
        """
        item_submissions, errors = self.prepare_batch(synced=synced)
        if errors:
            raise BatchCreationFailedError
        self._create_batch_in_db(item_submissions)

    @abstractmethod
    def prepare_batch(self, *, synced: bool = False) -> tuple[list, ...]:
        """Prepare batch submission assets in S3.

        This method performs the required steps to prepare a batch
        of item submissions in S3. These steps must include (at minimum)
        the following checks:

        - Check if there is metadata for the item submission;
          otherwise raise dsc.exceptions.ItemMetadataNotFoundError
        - Check if there are any bitstreams for the item submission;
          otherwise raise dsc.exceptions.ItemBitstreamsNotFoundError

        MUST be overridden by workflow subclasses.

        Returns:
            A tuple of item submissions (init params) represented as a
            list of dicts and errors represented as a list of tuples
            containing the item identifier and the error message.
        """
        pass  # noqa: PIE790

    @final
    def _create_batch_in_db(self, item_submissions: list[dict]) -> None:
        """Write records for a batch of item submissions to DynamoDB.

        This method loops through the item submissions (init params)
        represented as a list dicts. For each item submission, the
        method creates an instance of ItemSubmission and saves the
        record to DynamoDB.
        """
        for item_submission_init_params in item_submissions:
            item_submission = ItemSubmission.create(**item_submission_init_params)
            item_submission.last_run_date = self.run_date
            item_submission.status = ItemSubmissionStatus.BATCH_CREATED
            item_submission.save()

    @final
    def reconcile_items(self) -> bool:
        """Reconcile item submissions for a batch.

        This method loops through the item metadata, creating an
        instance of ItemSubmission with data loaded from a corresponding
        record in DynamoDB (if it exists). For each ItemSubmission,
        the method calls a workflow-specific reconcile method to
        determine if it includes the required submission assets (bitstreams
        and metadata), recording the results along the way.

        After going through each ItemSubmission, the method provides an overall
        summary of the results and returns a boolean indicating the status
        for the batch:
            - If any item submissions failed reconcile, returns False
            - If all item submissions were reconciled, returns True

        NOTE: This method is likely the first time a record will be inserted
        into DynamoDB for each item submission. If already present,
        its status will be updated.

        TODO: Reconcile methods will be deprecated after end-to-end testing.
        """
        reconciled_items = {}  # key=item_identifier, value=list of bitstream URIs
        bitstreams_without_metadata = []  # list of bitstream URIs
        metadata_without_bitstreams = []  # list of item identifiers

        # loop through each item metadata
        for item_metadata in self.item_metadata_iter():
            item_submission = ItemSubmission.get(
                batch_id=self.batch_id,
                item_identifier=item_metadata["item_identifier"],
            )

            # if no corresponding record in DynamoDB, skip
            if not item_submission:
                continue

            # attach source metadata
            item_submission.source_metadata = item_metadata

            # get reconcile status and status details
            status_details = None
            try:
                self.reconcile_item(item_submission)
            except ReconcileFailedError as exception:
                reconcile_status = ItemSubmissionStatus.RECONCILE_FAILED
                status_details = str(exception)

                if isinstance(exception, ReconcileFailedMissingBitstreamsError):
                    metadata_without_bitstreams.append(item_submission.item_identifier)
            else:
                reconcile_status = ItemSubmissionStatus.RECONCILE_SUCCESS
                reconciled_items[item_submission.item_identifier] = (
                    self.get_item_bitstream_uris(item_submission.item_identifier)
                )

            # update the table if not yet reconciled
            if item_submission.status in [
                None,
                ItemSubmissionStatus.RECONCILE_FAILED,
            ]:
                item_submission.last_run_date = self.run_date
                item_submission.status = reconcile_status
                item_submission.status_details = status_details
                item_submission.upsert_db()

                logger.debug(
                    "Updated status for the item submission(item_identifier="
                    f"{item_submission.item_identifier}): {item_submission.status}"
                )

        # check for unmatched bitstreams
        matched_bitstream_uris = reconciled_items.values()
        bitstreams_without_metadata.extend(
            list(
                set(self.batch_bitstream_uris)
                - set(itertools.chain(*matched_bitstream_uris))
            )
        )

        # log results
        reconcile_summary = {
            "reconciled": len(reconciled_items),
            "bitstreams_without_metadata": len(bitstreams_without_metadata),
            "metadata_without_bitstreams": len(metadata_without_bitstreams),
        }
        logger.info(
            f"Ran reconcile for batch '{self.batch_id}': {json.dumps(reconcile_summary)}"
        )
        if any((bitstreams_without_metadata, metadata_without_bitstreams)):
            logger.warning("Failed to reconcile bitstreams and metadata")

            if bitstreams_without_metadata:
                logger.warning(
                    ReconcileFoundBitstreamsWithoutMetadataWarning(
                        bitstreams_without_metadata
                    )
                )

            if metadata_without_bitstreams:
                logger.warning(
                    ReconcileFoundMetadataWithoutBitstreamsWarning(
                        metadata_without_bitstreams
                    )
                )
            return False

        logger.info(
            "Successfully reconciled bitstreams and metadata for all "
            f"{len(reconciled_items)} item(s)"
        )
        return True

    def reconcile_item(self, _item_submission: ItemSubmission) -> bool:
        """Reconcile bitstreams and metadata for an item.

        Items in DSpace represent a "work" and combine metadata and files,
        known as "bitstreams". For any given workflow, this method ensures
        the existence of both bitstreams and metadata for each item in the
        batch, verifying that all provided bitstreams can be linked to a
        metadata record and vice versa.

        If an item fails reconcile, this method should raise
        dsc.exceptions.ReconcileFailed*Error. Otherwise, return True.

        TODO: Reconcile methods will be deprecated after end-to-end testing.
        """
        return False

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
                item_submission.bitstream_s3_uris = self.get_item_bitstream_uris(
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
                self.submission_summary["submitted"] += 1

                logger.info(f"Sent item submission message: {item_data["message_id"]}")

                # Set status in DynamoDB
                item_submission.status = ItemSubmissionStatus.SUBMIT_SUCCESS
                item_submission.submit_attempts += 1
                item_submission.upsert_db()
            except Exception as exception:  # noqa: BLE001
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

    @final
    def finalize_items(self) -> None:
        """Examine results for all item submissions in the batch.

        This method involves three main steps:

        1. Process DSS result messages from the output queue
        2. Apply workflow-specific processing
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
            message_body=report.generate_summary(),
            attachments=[
                (
                    f"{self.batch_id}-item-submissions.csv",
                    report.write_item_submissions_csv(StringIO()),
                )
            ],
        )
