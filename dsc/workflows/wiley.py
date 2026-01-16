import concurrent.futures
import csv
import datetime
import inspect
import json
import jsonlines
import logging
from itertools import chain
from typing import Any, Iterable

import pandas as pd
import requests
import smart_open

from dsc.config import Config
from dsc.db.models import ItemSubmissionDB, ItemSubmissionStatus
from dsc.exceptions import (
    BatchCreationFailedError,
    ItemBitstreamsNotFoundError,
    ItemIdentifiersFileNotFoundError,
    ItemMetadataNotFoundError,
)
from dsc.workflows import Workflow
from dsc.utilities.aws.s3 import S3Client

logger = logging.getLogger(__name__)
CONFIG = Config()

WILEY_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36"
}


class WileyTransformer:
    """Transformer for Wileuy source metadata from Crossref API."""

    fields: Iterable[str] = [
        # fields with derived values
        "dc_title",  # title
        "dc_date_issued",  # issued
        "dc_contributor_author",  # author
        "dc_title_alternative",  # original-title, short-title, subtitle
        # fields with static values
        "dc_publisher",  # publisher
        "dc_identifier_issn",  # ISSN
        "dc_relation_journal",  # container-title
        "mit_journal_volume",  # volume
        "mit_journal_issue",  # issue,
        "dc_language",  # language
        "dc_relation_isversionof",  # URL
    ]

    @classmethod
    def transform(cls, source_metadata: dict) -> dict:
        """Transform source metadata."""
        transformed_metadata: dict[str, Any] = {}

        if not source_metadata:
            return transformed_metadata

        for field in cls.fields:
            field_method = getattr(cls, field)
            formatted_field_name = field.replace("_", ".")

            # check if 'source_metadata' is in signature
            signature = inspect.signature(field_method)
            if "source_metadata" in signature.parameters:
                transformed_metadata[formatted_field_name] = field_method(source_metadata)
            else:
                transformed_metadata[formatted_field_name] = field_method()

        return transformed_metadata

    @classmethod
    def dc_title(cls, source_metadata: dict) -> str:
        """Build a title string from title components."""
        return ". ".join(
            title_components for title_components in source_metadata["title"]
        )

    @classmethod
    def dc_date_issued(cls, source_metadata: dict) -> str:
        """Return a date string using date components from 'issued' field.

        If day is not provided in the metadata, a date string formatted as
        "%Y-%m" (no day) is returned.

        Example:
            Input: {"issued": {'date-parts': [[2019, 2, 8]]}}
            Output: "2019-02-08"
        """
        date_components = dict(
            zip(("year", "month", "day"), source_metadata["issued"]["date-parts"][0])
        )
        if date_components.get("day"):
            date = datetime.date(*date_components.values())
            return date.strftime("%Y-%m-%d")

        date = datetime.date(
            date_components["year"], date_components["month"], 1
        )  # day is required, use 1
        return date.strftime("%Y-%m")

    @classmethod
    def dc_contributor_author(cls, source_metadata: dict) -> list[str] | None:
        """Return a list of formatted instructor names.

        Example:
            Input: {"author": [{"given": "Marsha", "family": "Mellow", ...}]}
            Output: "Mellow, Marsha"
        """
        return [
            author_name
            for author in source_metadata["author"]
            if (author_name := cls._format_author_name(author))
        ] or None

    @classmethod
    def _format_author_name(cls, name_components: dict[str, str]) -> str:
        """Format author name as 'family, given'.

        Example:
            Input: {"given": "Marsha", "family": "Mellow}
            Output: "Mellow, Marsha"
        """
        if not (family := name_components.get("family")) or not (
            given := name_components.get("given")
        ):
            return ""
        author_name = f"{family}, {given}"
        return author_name.strip()

    @classmethod
    def dc_title_alternative(cls, source_metadata: dict) -> list[str] | None:
        """Return a list of alternative titles from multiple list fields."""

        alternative_title_lists = [
            source_metadata.get("original-title"),
            source_metadata.get("short-title"),
            source_metadata.get("subtitle"),
        ]

        return (
            list(
                chain.from_iterable(
                    alternative_titles
                    for alternative_titles in alternative_title_lists
                    if alternative_titles
                )
            )
            or None
        )

    @classmethod
    def dc_publisher(cls, source_metadata: dict) -> str | None:
        return source_metadata.get("publisher")

    @classmethod
    def dc_identifier_issn(cls, source_metadata: dict) -> list[str] | None:
        return source_metadata.get("ISSN")

    @classmethod
    def dc_relation_journal(cls, source_metadata: dict) -> list[str] | None:
        return source_metadata.get("container-title")

    @classmethod
    def mit_journal_volume(cls, source_metadata: dict) -> str | None:
        return source_metadata.get("volume")

    @classmethod
    def mit_journal_issue(cls, source_metadata: dict) -> str | None:
        return source_metadata.get("issue")

    @classmethod
    def dc_language(cls, source_metadata: dict) -> str | None:
        return source_metadata.get("language")

    @classmethod
    def dc_relation_isversionof(cls, source_metadata: dict) -> str | None:
        return source_metadata.get("dc_relation_isversionof")


class Wiley(Workflow):
    workflow_name: str = "wiley"
    metadata_transformer = WileyTransformer

    @property
    def metadata_mapping_path(self) -> str:
        return "dsc/workflows/metadata_mapping/wiley.json"

    def get_batch_bitstream_uris(self) -> list[str]:
        return list(
            S3Client().files_iter(
                bucket=self.s3_bucket,
                prefix=self.batch_path,
                file_type=".pdf",
                exclude_prefixes=self.exclude_prefixes,
            )
        )

    def item_metadata_iter(self, metadata_file: str = "metadata.jsonl"):
        """Yield item metadata from metadata JSONL file."""
        with jsonlines.Reader(
            smart_open.open(
                f"s3://{self.s3_bucket}/{self.batch_path}{metadata_file}", "r"
            )
        ) as reader:
            yield from reader.iter(type=dict)

    def create_batch(self, ids_file: str | None = None, *, synced: bool = False) -> None:
        """Create a batch of item submissions for processing.

        A "batch" refers to a collection of item submissions that are grouped together
        for coordinated processing, storage, and workflow execution. Each batch
        typically consists of multiple items, each with its own metadata and
        associated files, organized under a unique batch identifier.

        This method prepares the necessary assets in S3 (programmatically as needed)
        and records each item in the batch to DynamoDB.
        """
        logger.info(f"Creating batch '{self.batch_id}'")
        item_submissions, errors = self.prepare_batch(ids_file=ids_file, synced=synced)
        if errors:
            logger.error(errors)
        self._create_batch_in_db(item_submissions)

    def prepare_batch(
        self,
        ids_file: str | None = None,
        *,
        synced: bool = False,  # noqa: ARG002
    ) -> tuple[list, ...]:
        item_submissions = []
        errors = []

        if ids_file is None:
            # NOTE: errors normally take the format of (<item_identifier>, <error_message>)
            #       but this error is not at the item level
            errors.append((None, str(ItemIdentifiersFileNotFoundError())))
            return item_submissions, errors

        item_identifiers = self._get_item_identifiers(ids_file)
        self.create_metadata_file(item_identifiers)
        self.download_bitstreams(item_identifiers)

        for item_metadata in self.item_metadata_iter():
            # check if metadata is provided
            # item identifier is always returned by iter
            if len(item_metadata) == 1 and "item_identifier" in item_metadata:
                error = str(ItemMetadataNotFoundError())
                logger.error(
                    f"Failed to create item {item_metadata['item_identifier']}: {error})"
                )
                errors.append((item_metadata["item_identifier"], error))
                continue

            # check if there are any bitstreams associated with the item submission
            if not self.get_item_bitstream_uris(
                item_identifier=item_metadata["item_identifier"]
            ):
                error = str(ItemBitstreamsNotFoundError())
                logger.error(
                    f"Failed to create item {item_metadata['item_identifier']}: {error})"
                )
                errors.append((item_metadata["item_identifier"], error))
                continue

            # if item submission has associated bitstreams, save init params
            item_submissions.append(
                {
                    "batch_id": self.batch_id,
                    "item_identifier": item_metadata["item_identifier"],
                    "workflow_name": self.workflow_name,
                }
            )

        return item_submissions, errors

    def _get_item_identifiers(self, item_identifiers_file: str) -> list[str]:
        with smart_open.open(
            f"s3://{self.s3_bucket}/{self.workflow_name}/{item_identifiers_file}"
        ) as csvfile:
            # retrieve item identifiers (DOIs) from the CSV file
            metadata_df = pd.read_csv(
                csvfile, header=None, names=["item_identifier"], dtype="str"
            )

            # drop any rows where all values are missing
            metadata_df = metadata_df.dropna(how="all")

            return metadata_df["item_identifier"].to_list()

    def _get_item_submission(self, item_identifier: str):
        # TODO: YES THIS IS CONFUSING
        for item_submission in ItemSubmissionDB.scan(
            ItemSubmissionDB.item_identifier == item_identifier
        ):
            return item_submission

    def create_metadata_file(self, item_identifiers: list[str]) -> None:
        logger.info("Creating metadata file")
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            results = executor.map(self._get_crossref_metadata, item_identifiers)
            metadata = []
            for result in results:
                if result:
                    metadata.append(result)
            self._write_metadata_file_to_s3(metadata)

    def _get_crossref_metadata(self, doi: str) -> dict:
        url = f"{CONFIG.metadata_api_url}{doi}"
        logger.debug(f"Requesting metadata for {url}")

        # TODO: not the best way to check item status in DynamoDB
        #       explore Global Secondary Index (GSI)
        if item_submission := self._get_item_submission(doi.replace("/", "-")):
            if item_submission.status == ItemSubmissionStatus.INGEST_SUCCESS:
                logger.info(f"Record {doi} already ingested")
                return

        try:
            response = requests.get(
                url, params={"mailto": "dspace-lib@mit.edu"}, timeout=30
            )
        except Exception as exception:
            logger.error(f"Failed to retrieve metadata for {url}: {exception}")
            return {"item_identifier": doi}

        try:
            source_metadata = response.json()
        except json.JSONDecodeError:
            logger.error(f"Failed to parse response for {url}")
            return {"item_identifier": doi}

        logger.debug(f"Transforming source metadata for {doi}")
        transformed_metadata = self.metadata_transformer.transform(
            source_metadata.get("message", {})
        )
        return {"item_identifier": doi.replace("/", "-"), **transformed_metadata}

    def _write_metadata_file_to_s3(self, metadata: list[dict]):
        """Write single JSONLines file with retrieved metadata."""
        logger.debug(f"Writing metadata file to S3")
        with jsonlines.Writer(
            smart_open.open(f"s3://{self.s3_bucket}/{self.batch_path}metadata.jsonl", "w")
        ) as writer:
            writer.write_all(metadata)

        logger.info("Completed!")

    def download_bitstreams(self, item_identifiers: list[str]) -> None:
        logger.info("Downloading content from Wiley")
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(self._get_content, item_identifier)
                for item_identifier in item_identifiers
            ]

            total_futures = len(futures)
            errors = 0
            for future in concurrent.futures.as_completed(futures):
                content = future.result()
                if content is None:
                    errors += 1

            if errors > 0:
                logger.info(
                    f"Failed to download {errors}/{total_futures} expected bitstreams"
                )

    def _get_content(self, doi: str) -> bytes | Any | None:
        url = f"{CONFIG.content_api_url}{doi}"
        logger.debug(f"Requesting PDF for {url}")

        # TODO: not the best way to check item status in DynamoDB
        #       explore Global Secondary Index (GSI)
        if item_submission := self._get_item_submission(doi.replace("/", "-")):
            if item_submission.status == ItemSubmissionStatus.INGEST_SUCCESS:
                logger.info(f"Record {doi} already ingested")
                return

        try:
            response = requests.get(url, headers=WILEY_HEADERS, timeout=30)
            response.raise_for_status()
        except Exception as exception:
            logger.error(f"Failed to retrieve content for {url}: {exception}")
            return None

        if not response.headers["content-type"].startswith("application/pdf"):
            return None

        S3Client().put_file(
            file_content=response.content,
            bucket=self.s3_bucket,
            key=f"{self.batch_path}{doi.replace('/', '-')}.pdf",
        )

        return response.content
