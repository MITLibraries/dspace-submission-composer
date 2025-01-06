from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from importlib import import_module
from typing import TYPE_CHECKING, Any, final

from dsc.config import WORKFLOWS
from dsc.exceptions import (
    InvalidDSpaceMetadataError,
    ItemMetadatMissingRequiredFieldError,
)
from dsc.item_submission import ItemSubmission

if TYPE_CHECKING:
    from collections.abc import Iterator

    from mypy_boto3_sqs.type_defs import SendMessageResultTypeDef

logger = logging.getLogger(__name__)


class BaseWorkflow(ABC):
    """A base workflow class from which other workflow classes are derived."""

    workflow_name: str = "base"
    submission_system: str = "DSpace@MIT"
    email_recipients: tuple[str] = ("None",)
    metadata_mapping_path: str = ""
    s3_bucket: str = ""
    output_queue: str = ""

    def __init__(
        self,
        collection_handle: str,
        batch_id: str,
    ) -> None:
        """Initialize base instance.

        Args:
            collection_handle: The handle of the DSpace collection to which
                submissions will be uploaded.
            batch_id: Unique identifier for a 'batch' deposit that corresponds
                to the name of a subfolder in the workflow directory of the S3 bucket.
                This subfolder is where the S3 client will search for bitstream
                and metadata files.
        """
        self.batch_id = batch_id
        self.collection_handle = collection_handle

    @property
    def batch_path(self) -> str:
        return f"{self.workflow_name}/{self.batch_id}"

    @property
    def metadata_mapping(self) -> dict:
        with open(self.metadata_mapping_path) as mapping_file:
            return json.load(mapping_file)

    @final
    @classmethod
    def load(
        cls, workflow_name: str, collection_handle: str, batch_id: str
    ) -> BaseWorkflow:
        """Return configured workflow class instance.

        Args:
            workflow_name: The label of the workflow. Must match a key from
            config.WORKFLOWS.
            collection_handle: The handle of the DSpace collection to which the batch will
            be submitted.
            batch_id: The S3 prefix for the batch of DSpace submissions.
        """
        workflow_class = cls.get_workflow(workflow_name)
        return workflow_class(
            collection_handle=collection_handle,
            batch_id=batch_id,
        )

    @final
    @classmethod
    def get_workflow(cls, workflow_name: str) -> type[BaseWorkflow]:
        """Return workflow class.

        Args:
            workflow_name: The label of the workflow. Must match a key from
            config.WORKFLOWS.
        """
        module_name, class_name = WORKFLOWS[workflow_name]["workflow-path"].rsplit(".", 1)
        source_module = import_module(module_name)
        return getattr(source_module, class_name)

    @final
    def run(self) -> Iterator[SendMessageResultTypeDef]:
        """Run workflow to submit items to  the DSpace Submission Service."""
        for item_submission in self.item_submissions_iter():
            item_submission.upload_dspace_metadata(self.s3_bucket, self.batch_path)
            response = item_submission.send_submission_message(
                self.workflow_name,
                self.output_queue,
                self.submission_system,
                self.collection_handle,
            )
            yield response

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
    def get_bitstream_s3_uris(self, item_identifier: str) -> list[str]:
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
