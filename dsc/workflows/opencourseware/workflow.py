import json
import logging
import zipfile
from collections.abc import Iterator
from typing import Any

import smart_open

from dsc.exceptions import ItemMetadataNotFoundError
from dsc.utilities.aws.s3 import S3Client
from dsc.workflows.base import Workflow
from dsc.workflows.opencourseware import OpenCourseWareTransformer

logger = logging.getLogger(__name__)


class OpenCourseWare(Workflow):
    """Workflow for OpenCourseWare (OCW) deposits.

    The deposits managed by this workflow are requested by the
    Scholarly Communications and Collections Strategy (SCCS) department
    and were previously deposited into DSpace@MIT by Technical services staff.
    """

    workflow_name: str = "opencourseware"
    metadata_transformer = OpenCourseWareTransformer

    @property
    def metadata_mapping_path(self) -> str:
        return "dsc/workflows/opencourseware/metadata_mapping.json"

    def get_batch_bitstream_uris(self) -> list[str]:
        """Get list of URIs for all zipfiles within the batch folder."""
        s3_client = S3Client()
        return list(
            s3_client.files_iter(
                bucket=self.s3_bucket,
                prefix=self.batch_path,
                file_type=".zip",
                exclude_prefixes=self.exclude_prefixes,
            )
        )

    def item_metadata_iter(self) -> Iterator[dict[str, Any]]:
        """Yield item metadata from metadata JSON file in the zip file.

        If the zip file does not include a metadata JSON file (data.json),
        this method yields a dict containing only the item identifier.
        Otherwise, a dict containing the item identifier and transformed metadata
        is yielded.

        NOTE: Item identifiers are retrieved from the filenames of the zip
        files, which follow the naming format "<item_identifier>.zip".
        """
        for file in self.batch_bitstream_uris:
            try:
                source_metadata = self._read_metadata_from_zip_file(file)
            except FileNotFoundError:
                source_metadata = {}

            transformed_metadata = self.metadata_transformer.transform(source_metadata)

            yield {
                "item_identifier": self._parse_item_identifier(file),
                **transformed_metadata,
            }

    def _read_metadata_from_zip_file(self, file: str) -> dict[str, str]:
        """Read source metadata JSON file in zip archive.

        This method expects a JSON file called "data.json" at the root
        level of the the zip file.

        Args:
            file: Object prefix for bitstream zip file, formatted as the
                path from the S3 bucket to the file.
                Given an S3 URI "s3://dsc/opencourseware/batch-00/123.zip",
                then file = "opencourseware/batch-00/123.zip".
        """
        with (
            smart_open.open(file, "rb") as file_input,
            zipfile.ZipFile(file_input) as zip_file,
            zip_file.open("data.json") as json_file,
        ):
            return json.load(json_file)

    def _parse_item_identifier(self, file: str) -> str:
        """Parse item identifier from bitstream zip file."""
        return file.split("/")[-1].removesuffix(".zip")

    def prepare_batch(
        self,
        *,
        synced: bool = False,  # noqa: ARG002
    ) -> tuple[list, ...]:
        """Prepare a batch of item submissions, given a batch of zip files.

        For this workflow, the expected number of item submissions is determined
        by the number of zip files in the batch folder. This method will iterate
        over the yielded transformed metadata, checking whether metadata is provided:

        - If only the item identifier is provided and no other metadata is available,
          an error is recorded
        - If metadata is present, init params are generated for the item submission

        For the OpenCourseWare workflow, the batch preparation steps are the same
        for synced vs. non-synced workflows.
        """
        item_submissions = []
        errors = []

        for item_metadata in self.item_metadata_iter():
            # check if metadata is provided
            # item identifier is always returned by iter
            if len(item_metadata) == 1 and "item_identifier" in item_metadata:
                errors.append(
                    (item_metadata["item_identifier"], str(ItemMetadataNotFoundError()))
                )
                continue

            # if item submission includes metadata, save init params
            item_submissions.append(
                {
                    "batch_id": self.batch_id,
                    "item_identifier": item_metadata["item_identifier"],
                    "workflow_name": self.workflow_name,
                }
            )

        return item_submissions, errors
