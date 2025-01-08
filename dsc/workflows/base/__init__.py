from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import TYPE_CHECKING, Any, final

from dsc.exceptions import (
    InvalidDSpaceMetadataError,
    InvalidWorkflowNameError,
    ItemMetadatMissingRequiredFieldError,
)
from dsc.item_submission import ItemSubmission
from dsc.utilities.aws.s3 import S3Client

if TYPE_CHECKING:
    from _collections_abc import dict_keys
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
            workflow_name: The label of the workflow. Must match a workflow_name attribute
            from BaseWorkflow subclass.
        """
        for workflow_class in BaseWorkflow.__subclasses__():
            if workflow_name == workflow_class.workflow_name:
                return workflow_class
        raise InvalidWorkflowNameError(f"Invalid workflow name: {workflow_name} ")

    def reconcile_bitstreams_and_metadata(self) -> tuple[set[str], set[str]]:
        """Reconcile bitstreams against metadata.

        Generate a list of bitstreams without item identifiers and item identifiers
        without bitstreams. Any discrepancies will be addressed by the engineer and
        stakeholders as necessary.
        """
        bitstream_dict = self._build_bitstream_dict()

        # extract item identifiers from batch metadata
        item_identifiers = [
            self.get_item_identifier(item_metadata)
            for item_metadata in self.item_metadata_iter()
        ]

        # reconcile item identifiers against bitstreams
        item_identifiers_with_bitstream_matches = (
            self._match_item_identifiers_to_bitstreams(
                bitstream_dict.keys(), item_identifiers
            )
        )
        file_matches = self._match_bitstreams_to_item_identifiers(
            bitstream_dict.keys(), item_identifiers
        )
        logger.info(
            "Item identifiers from batch metadata with matching bitstreams: "
            f"{item_identifiers_with_bitstream_matches}"
        )
        bitstreams_with_item_identifier_matches = set(item_identifiers) - set(
            item_identifiers_with_bitstream_matches
        )
        no_item_identifiers = set(bitstream_dict.keys()) - set(file_matches)
        return bitstreams_with_item_identifier_matches, no_item_identifiers

    def _build_bitstream_dict(self) -> dict:
        """Build a dict of potential bitstreams with an item identifier for the key.

        An underscore (if present) serves as the delimiter between the item identifier
        and any additional suffixes in the case of multiple matching bitstreams.
        """
        s3_client = S3Client()
        bitstreams = list(
            s3_client.files_iter(bucket=self.s3_bucket, prefix=self.batch_path)
        )
        bitstream_dict: dict[str, list[str]] = defaultdict(list)
        for bitstream in bitstreams:
            file_name = bitstream.split("/")[-1]
            item_identifier = file_name.split("_")[0] if "_" in file_name else file_name
            bitstream_dict[item_identifier].append(bitstream)
        return bitstream_dict

    def _match_bitstreams_to_item_identifiers(
        self, bitstreams: dict_keys, item_identifiers: list[str]
    ) -> list[str]:
        """Create list of bitstreams matched to item identifiers.

        Args:
            bitstreams: A dict of S3 files with base file IDs and full URIs.
            item_identifiers: A list of item identifiers retrieved from the batch
            metadata.
        """
        return [
            file_id
            for item_identifier in item_identifiers
            for file_id in bitstreams
            if file_id == item_identifier
        ]

    def _match_item_identifiers_to_bitstreams(
        self, bitstreams: dict_keys, item_identifiers: list[str]
    ) -> list[str]:
        """Create list of item identifers matched to bitstreams.

        Args:
            bitstreams: A dict of S3 files with base file IDs and full URIs.
            item_identifiers: A list of item identifiers retrieved from the batch
            metadata.
        """
        return [
            item_identifier
            for file_id in bitstreams
            for item_identifier in item_identifiers
            if file_id == item_identifier
        ]

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
