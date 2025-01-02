from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, final

from dsc.exceptions import (
    InvalidDSpaceMetadataError,
    ItemMetadatMissingRequiredFieldError,
)
from dsc.item_submission import ItemSubmission

if TYPE_CHECKING:
    from collections.abc import Iterator

logger = logging.getLogger(__name__)


class BaseWorkflow(ABC):
    """A base workflow class from which other workflow classes are derived."""

    def __init__(
        self,
        workflow_name: str,
        submission_system: str,
        email_recipients: list[str],
        metadata_mapping: dict,
        s3_bucket: str,
        batch_id: str,
        collection_handle: str,
        output_queue: str,
    ) -> None:
        """Initialize base instance.

        Args:
            workflow_name (str): The name of the workflow.
            submission_system (str): The system to which item submissions will be sent
                (e.g. DSpace@MIT).
            email_recipients (list[str]): The email addresses to notify after runs of
                the workflow.
            metadata_mapping (dict):  A mapping file for generating DSpace metadata
                from the workflow's source metadata.
            s3_bucket (str): The S3 bucket containing bitstream and metadata files for
                the workflow.
            batch_id (str): Unique identifier for a 'batch' deposit that corresponds
                to the name of a subfolder in the workflow directory of the S3 bucket.
                This subfolder is where the S3 client will search for bitstream
                and metadata files.
            collection_handle (str): The handle of the DSpace collection to which
                submissions will be uploaded.
            output_queue (str): The SQS output queue used for retrieving result messages
                from the workflow's submissions.
        """
        self.workflow_name: str = workflow_name
        self.submission_system: str = submission_system
        self.email_recipients: list[str] = email_recipients
        self.metadata_mapping: dict = metadata_mapping
        self.s3_bucket: str = s3_bucket
        self.batch_id: str = batch_id
        self.collection_handle: str = collection_handle
        self.output_queue: str = output_queue

    @property
    def batch_path(self) -> str:
        return f"{self.workflow_name}/{self.batch_id}"

    @final  # noqa: B027
    def run(self) -> None:
        """Run workflow to submit items to  the DSpace Submission Service.

        PLANNED CODE:

        sqs_client = SQSClient()
        for item_submission in self.item_submissions_iter():
            item_submission.upload_dspace_metadata(
                self.s3_bucket, item_submission.metadata_s3_key
            )
            sqs_client.send_submission_message(
                item_submission.item_identifier,
                self.workflow_name,
                self.output_queue,
                self.submission_system,
                self.collection_handle,
                item_submission.metadata_uri,
                item_submission.bitstream_uris,
            )
        """

    @final
    def item_submissions_iter(self) -> Iterator[ItemSubmission]:
        """Yield item submissions for the DSpace Submission Service.

        MUST NOT be overridden by workflow subclasses.
        """
        for item_metadata in self.item_metadata_iter():
            item_identifier = self.get_item_identifier(item_metadata)
            logger.info(f"Processing submission for '{item_identifier}'")
            metadata_s3_key = f"{self.batch_path}/{item_identifier}_metadata.json"
            dspace_metadata = self.create_dspace_metadata(item_metadata)
            self.validate_dspace_metadata(dspace_metadata)
            item_submission = ItemSubmission(
                dspace_metadata=dspace_metadata,
                bitstream_uris=self.get_bitstream_uris(item_identifier),
                metadata_s3_key=metadata_s3_key,
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
            "language": None,
            "delimiter": "|",
        }

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
                    delimiter = field_mapping["delimiter"]
                    language = field_mapping["language"]
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
    def get_bitstream_uris(self, item_identifier: str) -> list[str]:
        """Get bitstreams for an item submission according to the workflow subclass.

        MUST be overridden by workflow subclasses.

        Args:
            item_identifier: The identifier used for locating the item's bitstreams.
        """

    @abstractmethod
    def process_deposit_results(self) -> list[str]:
        """Process results generated by the deposit according to the workflow subclass.

        MUST be overridden by workflow subclasses.
        """
