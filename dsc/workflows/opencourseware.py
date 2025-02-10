import json
import logging
import zipfile
from collections.abc import Iterator
from typing import Any

import smart_open

from dsc.exceptions import ReconcileError
from dsc.utilities.aws.s3 import S3Client
from dsc.workflows.base import Workflow

logger = logging.getLogger(__name__)


class OpenCourseWare(Workflow):
    """Workflow for OpenCourseWare (OCW) deposits.

    The deposits managed by this workflow are requested by the
    Scholarly Communications and Collections Strategy (SCCS) department
    and were previously deposited into DSpace@MIT by Technical services staff.
    """

    workflow_name: str = "opencourseware"

    @property
    def metadata_mapping_path(self) -> str:
        return "dsc/workflows/metadata_mapping/opencourseware.json"

    @property
    def s3_bucket(self) -> str:
        return "awaiting AWS infrastructure"

    @property
    def output_queue(self) -> str:
        return "awaiting AWS infrastructure"

    def reconcile_bitstreams_and_metadata(self) -> None:
        """Reconcile bitstreams against item metadata.

        Generate a list of bitstreams without item metadata.

        For OpenCourseWare deposits, the zip files are the bitstreams to be deposited
        into DSpace, but they also must contain a 'data.json' file, representing the
        metadata. As such, the 'reconcile' method only determines whether there are any
        bitstreams without metadata (any zip files without a 'data.json').
        Metadata without bitstreams is not calculated as for a 'data.json' file to
        exist, the zip file must also exist.
        """
        item_identifiers = []
        bitstreams_without_metadata = []
        s3_client = S3Client()
        for file in s3_client.files_iter(
            bucket=self.s3_bucket, prefix=self.batch_path, file_type=".zip"
        ):
            item_identifier = self.parse_item_identifier(file)
            item_identifiers.append(item_identifier)
            try:
                self._extract_metadata_from_zip_file(file)
            except FileNotFoundError:
                bitstreams_without_metadata.append(item_identifier)

        if any(bitstreams_without_metadata):
            reconcile_error_message = {
                "note": "Failed to reconcile bitstreams and metadata.",
                "bitstreams_without_metadata": {
                    "count": len(bitstreams_without_metadata),
                    "identifiers": bitstreams_without_metadata,
                },
            }
            logger.error(json.dumps(reconcile_error_message))
            raise ReconcileError(json.dumps(reconcile_error_message))

        logger.info(
            "Successfully reconciled bitstreams and metadata for all "
            f"items (n={len(item_identifiers)})."
        )

    def item_metadata_iter(self) -> Iterator[dict[str, Any]]:
        """Yield source metadata from metadata JSON file in the zip file.

        The item identifiers are retrieved from the filenames of the zip
        files, which follow the naming format "<item_identifier>.zip".
        """
        s3_client = S3Client()
        for file in s3_client.files_iter(
            bucket=self.s3_bucket, prefix=self.batch_path, file_type=".zip"
        ):
            yield {
                "item_identifier": self.parse_item_identifier(file),
                **self._extract_metadata_from_zip_file(file),
            }

    def _extract_metadata_from_zip_file(self, file: str) -> dict[str, str]:
        """Yield source metadata from metadata JSON file in zip archive.

        This method expects a JSON file called "data.json" at the root
        level of the the zip file.

        Args:
            file: Object prefix for bitstream zip file, formatted as the
                path from the S3 bucket to the file.
                Given an S3 URI "s3://dsc/opencourseware/batch-00/123.zip",
                then file = "opencourseware/batch-00/123.zip".

            item_identifier: Item identifier, used to find and read the metadata
                JSON file for the associated bitstream zip file.
        """
        zip_file_uri = f"s3://{self.s3_bucket}/{file}"
        with smart_open.open(zip_file_uri, "rb") as file_input, zipfile.ZipFile(
            file_input
        ) as zip_file:
            for filename in zip_file.namelist():
                if filename == "data.json":
                    return self._read_metadata_json_file(zip_file)
            raise FileNotFoundError(
                "The required file 'data.json' file was not found in the zip file: "
                f"{file}"
            )

    def _read_metadata_json_file(self, zip_file: zipfile.ZipFile) -> dict[str, str]:
        """Read source metadata JSON file."""
        with zip_file.open("data.json") as file:
            source_metadata = json.load(file)
            source_metadata["instructors"] = self._get_instructors_delimited_string(
                source_metadata["instructors"]
            )
            return source_metadata

    def _get_instructors_delimited_string(self, instructors: list[dict[str, str]]) -> str:
        """Get delimited string of 'instructors' from source metadata JSON file.

        Source metadata JSON files stored in OCW zip files contain an 'instructors'
        property, which contains an array of objects representing an instructor's
        credentials:

            [
                {
                    "first_name": "Kerry",
                    "last_name": "Oki",
                    "middle_initial": "",
                    "salutation": "Prof.",
                    "title": "Prof. Kerry Oki"
                },
                {
                    "first_name": "Earl",
                    "last_name": "Bird",
                    "middle_initial": "E.",
                    "salutation": "Prof.",
                    "title": "Prof. Earl E. Bird"
                }
            ]

        Given these credentials, this method will construct a pipe-delimited ("|")
        string with the following format: "<last_name>, <first_name> <middle_initial>".

            Example output:
                "Oki, Kerry|Bird, Earl E."

        """
        return "|".join(
            [
                instructor_name
                for instructor in instructors
                if (instructor_name := self._construct_instructor_name(instructor))
            ]
        ).strip()

    @staticmethod
    def _construct_instructor_name(instructor: dict[str, str]) -> str:
        """Given a dictionary of name fields, derive instructor name."""
        if not (last_name := instructor.get("last_name")) or not (
            first_name := instructor.get("first_name")
        ):
            return ""
        return f"{last_name}, {first_name} {instructor.get("middle_initial", "")}".strip()

    def get_item_identifier(self, item_metadata: dict[str, Any]) -> str:
        """Get 'item_identifier' from item metadata entry."""
        return item_metadata["item_identifier"]

    def parse_item_identifier(self, file: str) -> str:
        """Parse item identifier from bitstream zip file."""
        return file.split("/")[-1].removesuffix(".zip")

    def get_bitstream_s3_uris(self, item_identifier: str) -> list[str]:
        s3_client = S3Client()
        return list(
            s3_client.files_iter(
                bucket=self.s3_bucket,
                prefix=self.batch_path,
                item_identifier=item_identifier,
                file_type=".zip",
                exclude_prefixes=["archived"],
            )
        )
