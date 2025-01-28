import json
import logging
import zipfile
from collections import defaultdict
from collections.abc import Iterator
from typing import Any

import smart_open

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
    metadata_mapping_path: str = "dsc/workflows/metadata_mapping/opencourseware.json"

    def reconcile_bitstreams_and_metadata(self) -> tuple[set[str], set[str]]:
        """Reconcile bitstreams against item metadata.

        Generate a list of bitstreams without item metadata.

        For OpenCourseWare deposits, the zip files are the bitstreams to be deposited
        into DSpace, but they also must contain a 'data.json' file, which is the item
        metadata. As such, the 'reconcile' method only determines whether there are any
        bitstreams without item metadata (any zip files without a 'data.json').
        Item metadata without bitstreams are basically impossible because the item
        metadata ('data.json') is inside the bitstream (zip file),
        hence an empty set is returned.
        """
        bitstream_dict = self._build_bitstream_dict()

        # extract item identifiers from bitstream_dict
        item_identifiers = list(bitstream_dict.keys())

        bitstreams_without_metadata = set(item_identifiers) - set(
            self._identify_bitstreams_with_metadata(item_identifiers)
        )
        item_metadata_without_bitstreams: set[str] = set()
        return item_metadata_without_bitstreams, bitstreams_without_metadata

    def _build_bitstream_dict(self) -> dict:
        """Build dictionary of bitstreams.

        This method will look for zip files within the designated 'batch' folder
        of the S3 bucket (i.e., self.batch_path). In the case of OpenCourseWare
        deposits, this method expects:
            * a single (1) zipped file per item identifier
            * filename of zipped file must correspond to item identifier
              (i.e., '<item_identifier>'.zip)
        """
        s3_client = S3Client()
        bitstreams = list(
            s3_client.files_iter(
                bucket=self.s3_bucket, prefix=self.batch_path, file_type=".zip"
            )
        )
        bitstream_dict: dict[str, list[str]] = defaultdict(list)
        for bitstream in bitstreams:
            file_name = bitstream.split("/")[-1]
            item_identifier = file_name.removesuffix(".zip")
            bitstream_dict[item_identifier].append(bitstream)
        return bitstream_dict

    def _identify_bitstreams_with_metadata(
        self, item_identifiers: list[str]
    ) -> list[str]:
        bitstreams_with_metadata = []
        for item_identifier in item_identifiers:
            try:
                zip_file = (
                    f"s3://{self.s3_bucket}/{self.batch_path}/{item_identifier}.zip"
                )
                self._extract_metadata_from_zip_file(zip_file, item_identifier)
                bitstreams_with_metadata.append(item_identifier)
            except FileNotFoundError as exception:
                logger.error(exception)  # noqa: TRY400
        return bitstreams_with_metadata

    def item_metadata_iter(self) -> Iterator[dict[str, Any]]:
        """Yield source metadata from metadata JSON file in the zip file.

        The item identifiers are retrieved from the filenames of the zip
        files, which follow the naming format "<item_identifier>.zip".
        """
        s3_client = S3Client()
        for file in s3_client.files_iter(
            bucket=self.s3_bucket, prefix=self.batch_path, file_type=".zip"
        ):
            zip_file = f"s3://{self.s3_bucket}/{file}"
            item_identifier = file.split("/")[-1].removesuffix(".zip")
            yield {
                "item_identifier": item_identifier,
                **self._extract_metadata_from_zip_file(zip_file, item_identifier),
            }

    def _extract_metadata_from_zip_file(
        self, file: str, item_identifier: str
    ) -> dict[str, str]:
        """Yield source metadata from metadata JSON file in zip archive.

        This method expects a JSON file called "data.json" at the root
        level of the the zip file.
        """
        with smart_open.open(file, "rb") as file_input, zipfile.ZipFile(
            file_input
        ) as zip_file:
            for filename in zip_file.namelist():
                metadata_json = f"{item_identifier}/data.json"
                if filename == metadata_json:
                    return self._read_metadata_json_file(zip_file, metadata_json)
            raise FileNotFoundError(
                "The required file 'data.json' file was not found in the zip file: "
                f"{file}"
            )

    def _read_metadata_json_file(
        self, zip_file: zipfile.ZipFile, metadata_json: str
    ) -> dict[str, str]:
        """Read source metadata JSON file."""
        with zip_file.open(metadata_json) as file:
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

    def process_deposit_results(self) -> list[str]:
        """TODO: Stub method."""
        return [""]
