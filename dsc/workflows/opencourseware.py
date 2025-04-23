import json
import logging
import zipfile
from collections.abc import Iterator
from typing import Any

import smart_open

from dsc.exceptions import ReconcileFoundBitstreamsWithoutMetadataWarning
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

    def reconcile_bitstreams_and_metadata(self) -> bool:
        """Reconcile bitstreams against item metadata.

        Generate a list of bitstreams without item metadata.

        For OpenCourseWare deposits, the zip files are the bitstreams to be deposited
        into DSpace, but they also must contain a 'data.json' file, representing the
        metadata. As such, the 'reconcile' method only determines whether there are any
        bitstreams without metadata (any zip files without a 'data.json').
        Metadata without bitstreams is not calculated as for a 'data.json' file to
        exist, the zip file must also exist.
        """
        logger.info(f"Reconciling bitstreams and metadata for batch '{self.batch_id}'")
        reconciled: bool = False
        reconcile_summary = {
            "reconciled": 0,
            "bitstreams_without_metadata": 0,
        }

        reconciled_items = {}
        bitstreams_without_metadata = []
        s3_client = S3Client()

        for file in s3_client.files_iter(
            bucket=self.s3_bucket, prefix=self.batch_path, file_type=".zip"
        ):
            item_identifier = self.parse_item_identifier(file)

            try:
                self._extract_metadata_from_zip_file(file)
            except FileNotFoundError:
                bitstreams_without_metadata.append(item_identifier)
            else:
                reconciled_items[item_identifier] = file

        self.workflow_events.reconciled_items = reconciled_items
        reconcile_summary.update(
            {
                "reconciled": len(reconciled_items),
                "bitstreams_without_metadata": len(bitstreams_without_metadata),
            }
        )

        logger.info(f"Reconcile results: {json.dumps(reconcile_summary)}")

        if any(bitstreams_without_metadata):
            logger.warning("Failed to reconcile bitstreams and metadata")
            logger.warning(
                ReconcileFoundBitstreamsWithoutMetadataWarning(
                    bitstreams_without_metadata
                )
            )
            self.workflow_events.reconcile_errors["bitstreams_without_metadata"] = (
                bitstreams_without_metadata
            )
        else:
            reconciled = True
            logger.info(
                "Successfully reconciled bitstreams and metadata for all "
                f"{len(reconciled_items)} item(s)"
            )

        return reconciled

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
        with smart_open.open(file, "rb") as file_input, zipfile.ZipFile(
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
            source_metadata["instructors"] = self._get_instructors_list(
                source_metadata["instructors"]
            )
            source_metadata["topics"] = self._get_topics_list(source_metadata["topics"])
            return source_metadata

    def _get_instructors_list(self, instructors: list[dict[str, str]]) -> list[str]:
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
                ["Oki, Kerry", "Bird, Earl E."]

        """
        return [
            instructor_name
            for instructor in instructors
            if (instructor_name := self._construct_instructor_name(instructor))
        ]

    @staticmethod
    def _construct_instructor_name(instructor: dict[str, str]) -> str:
        """Given a dictionary of name fields, derive instructor name."""
        if not (last_name := instructor.get("last_name")) or not (
            first_name := instructor.get("first_name")
        ):
            return ""
        return f"{last_name}, {first_name} {instructor.get("middle_initial", "")}".strip()

    def _get_topics_list(self, topics: list[list[str]]) -> list[str]:
        """Get list of concatenated 'topics' from source metadata JSON file.

        Source metadata JSON files stored in OCW zip files contain a 'topics'
        property, which contains an array of arrays with string values
        representing topics:

            [
                ["Social Science","Economics","International Economics"],
                ["Social Science","Economics","Macroeconomics"]
            ]

        Given a list of topic terms (also stored as a list), this method will
        conctenate each set of topic terms, using a dash to separate each term
        with the following format: "<topic 1> - <topic 2> - <topic 3>".

            Example output:
                [
                    "Social Science - Economics - International Economics",
                    "Social Science - Economics - Macroeconomics"
                ]
        """
        topics_list = [" - ".join(topic_terms) for topic_terms in topics]
        return list(filter(None, topics_list))

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
