from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, final

import jsonschema
import jsonschema.exceptions

from dsc.config import Config
from dsc.db.models import ItemSubmissionDB, ItemSubmissionStatus
from dsc.exceptions import InvalidSQSMessageError, InvalidWorkflowNameError
from dsc.item_submission import ItemSubmission
from dsc.utilities.aws import SESClient
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
    def item_metadata_iter(self) -> Iterator[dict[str, Any]]:
        """Iterate through batch metadata to yield item metadata.

        MUST be overridden by workflow subclasses.
        """

    @final
    def reconcile_items(self) -> bool:
        """Reconcile item submissions for a batch.

        This method will first reconcile all bitstreams and metadata.
        It will then iterate through the item metadata, creating instances
        of the ItemSubmission class. For each instance, data will be loaded
        in from the corresponding record in DynamoDB; if the record is not yet
        recorded in DynamoDB, it will first create and save the record to the table.

        Depending on the "current status" of the record in DynamoDB
        (note that status=None for created records), the method will conditionally
        update the records in the table with the status of the reconcile.

        NOTE: This may not be the full set of item submissions in a batch
        as there may be bitstreams (intended for item submissions)
        for which an item identifier cannot be retrieved.
        """
        reconciled = self.reconcile_bitstreams_and_metadata()

        # iterate over the results of the reconcile
        # for all item submissions from the metadata
        for item_metadata in self.item_metadata_iter():
            # create or get existing ItemSubmission
            item_submission = ItemSubmission.get(
                batch_id=self.batch_id, item_identifier=item_metadata["item_identifier"]
            )
            if not item_submission:
                item_submission = ItemSubmission.create(
                    batch_id=self.batch_id,
                    item_identifier=item_metadata["item_identifier"],
                    workflow_name=self.workflow_name,
                )

            if item_submission.status not in [
                None,
                ItemSubmissionStatus.RECONCILE_FAILED,
            ]:
                continue

            # update reconciliation status
            item_submission.last_run_date = self.run_date
            if item_submission.item_identifier in self.workflow_events.reconciled_items:
                item_submission.status = ItemSubmissionStatus.RECONCILE_SUCCESS
            else:
                item_submission.status = ItemSubmissionStatus.RECONCILE_FAILED

            logger.debug(
                "Updating status for the item submission(item_identifier="
                f"{item_submission.item_identifier}): {item_submission.status}"
            )

            # save status update
            item_submission.upsert_db()

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
        self.workflow_specific_processing()

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
