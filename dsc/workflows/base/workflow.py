from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, final

import jsonschema
import jsonschema.exceptions

from dsc.config import Config
from dsc.db.models import ItemSubmissionStatus
from dsc.exceptions import (
    BatchCreationFailedError,
    DSpaceMetadataUploadError,
    InvalidSQSMessageError,
    InvalidWorkflowNameError,
    ItemMetadataMissingRequiredFieldError,
)
from dsc.item_submission import ItemSubmission
from dsc.reports import Report
from dsc.utilities.aws import S3Client, SESClient, SQSClient
from dsc.utilities.validate.schemas import RESULT_MESSAGE_ATTRIBUTES, RESULT_MESSAGE_BODY

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Iterator

    from mypy_boto3_sqs.type_defs import MessageTypeDef

    from dsc.reports import Report
    from dsc.workflows.base import Transformer

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
        self._batch_dspace_metadata_json_uris: list[str] | None = None

    @property
    @abstractmethod
    def metadata_transformer(self) -> type[Transformer]:
        """Transformer for source metadata."""

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
    def batch_dspace_metadata_json_uris(self) -> list[str]:
        if not self._batch_dspace_metadata_json_uris:
            s3_client = S3Client()
            self._batch_dspace_metadata_json_uris = list(
                s3_client.files_iter(
                    bucket=self.s3_bucket,
                    prefix=f"{self.batch_path}dspace_metadata/",
                    file_type=".json",
                )
            )
        return self._batch_dspace_metadata_json_uris

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

    @final
    def get_item_dspace_metadata_json_uri(self, item_identifier: str) -> str:
        return next(
            uri for uri in self.batch_dspace_metadata_json_uris if item_identifier in uri
        )

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
        item_submissions, errors = self._prepare_batch(synced=synced)
        if errors:
            raise BatchCreationFailedError(errors)

        _dspace_metadata_uris, errors = self._create_dspace_metadata_json(
            item_submissions
        )
        if errors:
            raise BatchCreationFailedError(errors)

        self._create_batch_in_db(item_submissions)

    @abstractmethod
    def _prepare_batch(self, *, synced: bool = False) -> tuple[list, ...]:
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
            A tuple containing list of ItemSubmission's and
            errors represented as a list of tuples
            containing the item identifier and the error message.
        """
        pass  # noqa: PIE790

    @final
    def _create_dspace_metadata_json(
        self, item_submissions: list[ItemSubmission]
    ) -> tuple[list, ...]:
        dspace_metadata_uris = []
        errors = []
        for item_submission in item_submissions:
            try:
                uri = item_submission.prepare_dspace_metadata(
                    self.s3_bucket, self.batch_path
                )
                dspace_metadata_uris.append(uri)
            except (
                DSpaceMetadataUploadError,
                ItemMetadataMissingRequiredFieldError,
            ) as exception:
                errors.append((item_submission.item_identifier, str(exception)))
        return dspace_metadata_uris, errors

    @final
    def _create_batch_in_db(self, item_submissions: list[ItemSubmission]) -> None:
        """Write records for a batch of item submissions to DynamoDB.

        For each ItemSubmission, the method updates the last_run_date,
        status, and status_details attributes and saves the
        record to DynamoDB.
        """
        for item_submission in item_submissions:
            item_submission.last_run_date = self.run_date
            item_submission.status = ItemSubmissionStatus.BATCH_CREATED
            item_submission.status_details = None
            item_submission.save()

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
                item_submission.metadata_s3_uri = self.get_item_dspace_metadata_json_uri(
                    item_identifier
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
                item_submission.status_details = None
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
                item_submission.status_details = None
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

    def send_report(self, report: Report, email_recipients: list[str]) -> None:
        """Send report as an email via SES."""
        logger.info(f"Sending report to recipients: {email_recipients}")
        ses_client = SESClient(region=CONFIG.aws_region_name)
        ses_client.create_and_send_email(
            subject=report.subject,
            source_email_address=CONFIG.source_email,
            recipient_email_addresses=email_recipients,
            message_body=report.generate_summary(),
            attachments=report.generate_attachments(),
        )
