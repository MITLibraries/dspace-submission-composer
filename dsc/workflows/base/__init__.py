from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, final

from dsc.config import Config
from dsc.exceptions import (
    InvalidDSpaceMetadataError,
    InvalidWorkflowNameError,
    ItemMetadatMissingRequiredFieldError,
)
from dsc.item_submission import ItemSubmission
from dsc.utilities.aws import SESClient, SQSClient

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Iterator

    from dsc.reports import Report

logger = logging.getLogger(__name__)
CONFIG = Config()


@dataclass
class WorkflowEvents:
    """Record of events during the execution of Workflow methods.

    This dataclass is designed to hold useful data used in reporting.
    It is comprised of three lists, which contain details
    about reconciled, submitted, and processed items -- aligning with
    the DSC CLI commands (reconcile, submit, and finalize). Error
    messages are also tracked in a list.
    """

    reconciled_items: list[str] = field(default_factory=list)
    submitted_items: list[str] = field(default_factory=list)
    processed_items: list[dict] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


class Workflow(ABC):
    """A base workflow class from which other workflow classes are derived."""

    workflow_name: str = "base"
    submission_system: str = "DSpace@MIT"

    def __init__(
        self,
        batch_id: str,
    ) -> None:
        """Initialize base instance.

        Args:
            batch_id: Unique identifier for a 'batch' deposit that corresponds
                to the name of a subfolder in the workflow directory of the S3 bucket.
                This subfolder is where the S3 client will search for bitstream
                and metadata files.
        """
        self.batch_id = batch_id
        self.workflow_events = WorkflowEvents()

    @property
    @abstractmethod
    def metadata_mapping_path(self) -> str:
        """Path to the JSON metadata mapping file for the workflow."""

    @property
    def metadata_mapping(self) -> dict:
        with open(self.metadata_mapping_path) as mapping_file:
            return json.load(mapping_file)

    @property
    @abstractmethod
    def s3_bucket(self) -> str:
        """The S3 bucket containing the DSpace submission files."""

    @property
    @abstractmethod
    def output_queue(self) -> str:
        """The SQS output queue for the DSS result messages."""

    @property
    def batch_path(self) -> str:
        return f"{self.workflow_name}/{self.batch_id}"

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
    def reconcile_bitstreams_and_metadata(self) -> None:
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
    def submit_items(self, collection_handle: str) -> dict:
        """Submit items to the DSpace Submission Service according to the workflow class.

        Args:
            collection_handle: The handle of the DSpace collection to which the batch will
              be submitted.

        Returns a dict with the submission results organized into succeeded and failed
        items.
        """
        items: dict[str, Any] = {"succeeded": {}, "failed": {}}
        for item_submission in self.item_submissions_iter():
            item_id = item_submission.item_identifier
            try:
                item_submission.upload_dspace_metadata(self.s3_bucket, self.batch_path)
                response = item_submission.send_submission_message(
                    self.workflow_name,
                    self.output_queue,
                    self.submission_system,
                    collection_handle,
                )
            except Exception as exception:
                logger.exception(f"Error processing submission: '{item_id}'")
                items["failed"][item_id] = exception
                continue
            status_code = response["ResponseMetadata"]["HTTPStatusCode"]
            if status_code != HTTPStatus.OK:
                items["failed"][item_id] = RuntimeError("Non OK HTTPStatus")
                continue
            items["succeeded"][item_id] = response["MessageId"]

        results = {
            "success": not items["failed"],
            "items_count": len(items["succeeded"]) + len(items["failed"]),
            "items": items,
        }
        logger.info(results)
        return results

    @final
    def item_submissions_iter(self) -> Iterator[ItemSubmission]:
        """Yield item submissions for the DSpace Submission Service.

        MUST NOT be overridden by workflow subclasses.
        """
        for item_metadata in self.item_metadata_iter():
            item_identifier = self.get_item_identifier(item_metadata)
            logger.info(f"Processing submission for '{item_identifier}'")
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
                    delimiter = field_mapping.get("delimiter")
                    language = field_mapping.get("language")
                    if delimiter:
                        metadata_entries.extend(
                            [
                                {
                                    "key": field_name,
                                    "value": value,
                                    "language": language,
                                }
                                for value in field_value.split(delimiter)
                            ]
                        )
                    else:
                        metadata_entries.append(
                            {
                                "key": field_name,
                                "value": field_value,
                                "language": language,
                            }
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
    def process_results(self) -> None:
        """Process DSS results from the workflow's output queue.

        Must NOT be overridden by workflow subclasses.
        """
        items = self.process_sqs_queue()
        self.workflow_specific_processing(items)

    def process_sqs_queue(self) -> list[dict]:
        """Process messages in DSS ouput queue to extract necessary data.

        May be overridden by workflow subclasses.
        """
        logger.debug(f"Processing result messages in '{self.output_queue}'")

        sqs_client = SQSClient(
            region=CONFIG.aws_region_name, queue_name=self.output_queue
        )

        items = []
        for sqs_message in sqs_client.receive():
            try:
                item_identifier, result_message_body = sqs_client.process_result_message(
                    sqs_message
                )
            except Exception:
                error_message = f"Error while processing SQS message: {sqs_message}"
                logger.exception(error_message)
                self.workflow_events.errors.append(error_message)
                continue

            # capture all processed items, whether ingested or not
            item_data = {
                "item_identifier": item_identifier,
                "result_message_body": result_message_body,
                "ingested": result_message_body["ResultType"] == "success",
            }
            self.workflow_events.processed_items.append(item_data)
            items.append(item_data)

            if not item_data["ingested"]:
                message = (
                    f"Item '{item_identifier}' did not ingest successfully: {sqs_message}"
                )
                logger.info(message)
                self.workflow_events.errors.append(message)

        logger.debug(f"Messages received and deleted from '{self.output_queue}'")
        return items

    def workflow_specific_processing(self, items: list[dict]) -> None:
        logger.info(
            f"No extra processing for {len(items)} items based on workflow: "
            f"'{self.workflow_name}' "
        )

    def send_report(
        self, report_class: type[Report], email_recipients: list[str]
    ) -> None:
        """Send report as an email via SES."""
        report = report_class.from_workflow(self)

        ses_client = SESClient(region=CONFIG.aws_region_name)
        ses_client.create_and_send_email(
            subject=report.subject,
            source_email_address=CONFIG.dsc_source_email,
            recipient_email_addresses=email_recipients,
            message_body_plain_text=report.to_plain_text(),
            message_body_html=report.to_html(),
            attachments=report.create_attachments(),
        )
