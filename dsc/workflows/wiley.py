import concurrent.futures
import csv
import datetime
import inspect
import json
import logging
from itertools import chain
from typing import Any, Iterable

import pandas as pd
import requests
import smart_open

from dsc.config import Config
from dsc.exceptions import (
    ItemBitstreamsNotFoundError,
    ItemIdentifiersFileNotFoundError,
    ItemMetadataNotFoundError,
)
from dsc.workflows import SimpleCSV
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


class Wiley(SimpleCSV):
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

    def get_item_identifiers(self, item_identifiers_file: str) -> list[str]:
        with smart_open.open(
            f"s3://{self.s3_bucket}/{self.workflow_name}/{item_identifiers_file}",  # TODO: Add 'workflow_path' property
        ) as csvfile:
            # retrieve item identifiers (DOIs) from the CSV file
            metadata_df = pd.read_csv(
                csvfile, header=None, names=["item_identifier"], dtype="str"
            )

            # drop any rows where all values are missing
            metadata_df = metadata_df.dropna(how="all")

            return metadata_df["item_identifier"].to_list()

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

        item_identifiers = self.get_item_identifiers(ids_file)
        self.create_metadata_file(item_identifiers)
        self.download_bitstreams(item_identifiers)

        for item_metadata in self.item_metadata_iter():
            # check if metadata is provided
            # item identifier is always returned by iter
            if len(item_metadata) == 1 and "item_identifier" in item_metadata:
                errors.append(
                    (item_metadata["item_identifier"], str(ItemMetadataNotFoundError()))
                )
                continue

            # check if there are any bitstreams associated with the item submission
            if not self.get_item_bitstream_uris(
                item_identifier=item_metadata["item_identifier"]
            ):
                errors.append(
                    (
                        item_metadata["item_identifier"],
                        str(ItemBitstreamsNotFoundError()),
                    )
                )
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

    def create_metadata_file(self, item_identifiers: list[str]) -> None:
        # TODO: Must be updated to only send a request to Crossref API
        #       if no entry in DynamoDB table with status>="ingest_success";
        logger.info("Creating metadata CSV file")
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            results = executor.map(self.get_crossref_metadata, item_identifiers)

            item_metadata = []
            for result in results:
                item_metadata.append(result)

        item_metadata_df = pd.DataFrame(item_metadata)
        item_metadata_df.to_csv(
            f"s3://{self.s3_bucket}/{self.batch_path}metadata.csv", index=False
        )

    def get_crossref_metadata(self, doi: str) -> dict:
        url = f"{CONFIG.metadata_api_url}{doi}"
        logger.debug("Requesting metadata for", url)

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
        return {"item_identifier": doi, **transformed_metadata}

    def download_bitstreams(self, item_identifiers: list[str]) -> None:
        # TODO: Must be updated to only send a request to Crossref API
        #       if no entry in DynamoDB table with status>="ingest_success";
        logger.info("Downloading content from Wiley")
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(self.get_content, item_identifier)
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

    def get_content(self, doi: str) -> bytes | Any | None:
        url = f"{CONFIG.content_api_url}{doi}"
        logger.debug("Requesting PDF for", url)

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
