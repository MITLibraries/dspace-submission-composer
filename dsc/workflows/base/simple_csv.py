import csv
import json
import logging
from collections.abc import Iterator
from typing import Any

import smart_open

from dsc.exceptions import ReconcileError
from dsc.utilities.aws import S3Client
from dsc.workflows.base import Workflow

logger = logging.getLogger(__name__)


class SimpleCSV(Workflow):
    """Base workflow for deposits that rely on a metadata CSV file.

    The metadata CSV file must be stored in a designated path for the
    deposit on S3.
    """

    workflow_name: str = "simple_csv"

    def reconcile_bitstreams_and_metadata(
        self, metadata_file: str = "metadata.csv"
    ) -> None:
        """Reconcile item metadata from metadata CSV file with bitstreams.

        For SimpleCSV workflows, bitstreams (files) and a metadata CSV file
        are uploaded to a designated batch folder on S3. The reconcile method
        ensures that every bitstream on S3 has metadata--a row in the metadata
        CSV file--associated with it and vice versa.
        """
        # get item identifiers from bitstreams and metadata file
        bitstream_item_identifiers = self._get_item_identifiers_from_bitstreams(
            metadata_file
        )
        metadata_item_identifiers = self._get_item_identifiers_from_metadata(
            metadata_file
        )

        # get matching item identifiers (IDs for items with both bitstreams and metadata)
        matching_item_identifiers = bitstream_item_identifiers & metadata_item_identifiers

        bitstreams_without_metadata = list(
            bitstream_item_identifiers - matching_item_identifiers
        )
        metadata_without_bitstreams = list(
            metadata_item_identifiers - matching_item_identifiers
        )

        if any((bitstreams_without_metadata, metadata_without_bitstreams)):
            reconcile_error_message = {
                "note": "Failed to reconcile bitstreams and metadata.",
                "bitstreams_without_metadata": {
                    "count": len(bitstreams_without_metadata),
                    "identifiers": bitstreams_without_metadata,
                },
                "metadata_without_bitstreams": {
                    "count": len(metadata_without_bitstreams),
                    "identifiers": metadata_without_bitstreams,
                },
            }
            logger.error(json.dumps(reconcile_error_message))
            raise ReconcileError(json.dumps(reconcile_error_message))

        logger.info(
            "Successfully reconciled bitstreams and metadata for all "
            f"items (n={len(matching_item_identifiers)})."
        )

    def _get_item_identifiers_from_bitstreams(
        self, metadata_file: str = "metadata.csv"
    ) -> set[str]:
        """Get set of item identifiers from bitstreams.

        Item identifiers are extracted from the bitstream filenames.
        """
        item_identifiers = set()
        s3_client = S3Client()
        bitstreams = list(
            s3_client.files_iter(
                bucket=self.s3_bucket,
                prefix=self.batch_path,
                exclude_prefixes=["archived", metadata_file],
            )
        )
        for bitstream in bitstreams:
            file_name = bitstream.split("/")[-1]
            item_identifiers.add(
                file_name.split("_")[0] if "_" in file_name else file_name
            )
        return item_identifiers

    def _get_item_identifiers_from_metadata(
        self, metadata_file: str = "metadata.csv"
    ) -> set[str]:
        """Get set of item identifiers from metadata file."""
        item_identifiers = set()
        item_identifiers.update(
            [
                self.get_item_identifier(item_metadata)
                for item_metadata in self.item_metadata_iter(metadata_file)
            ]
        )
        return item_identifiers

    def item_metadata_iter(
        self, metadata_file: str = "metadata.csv"
    ) -> Iterator[dict[str, Any]]:
        """Yield dicts of item metadata from metadata CSV file.

        Args:
            metadata_file: Metadata CSV filename with the filename extension
                (.csv) included. Defaults to 'metadata.csv'.

        Yields:
            Item metadata.
        """
        with smart_open.open(
            f"s3://{self.s3_bucket}/{self.batch_path}/{metadata_file}"
        ) as csvfile:
            yield from csv.DictReader(csvfile)

    def get_bitstream_s3_uris(self, item_identifier: str) -> list[str]:
        """Get S3 URIs for bitstreams for a given item.

        This method uses S3Client.files_iter to get a list of files
        on S3 stored at s3://bucket/prefix/ and includes the 'item_identifier'
        in the object key.

        - If the exact filename is provided as 'item_identifier', only
          one bitstream is retrieved.
        - If a prefix is provided as 'item_identifier', one or more
          bitstreams are retrieved.

        Args:
            item_identifier: Item identifier used to filter bitstreams.

        Returns:
            Bitstream URIs for a given item.
        """
        s3_client = S3Client()
        return list(
            s3_client.files_iter(
                bucket=self.s3_bucket,
                prefix=self.batch_path,
                item_identifier=item_identifier,
                exclude_prefixes=["archived"],
            )
        )

    def get_item_identifier(self, item_metadata: dict[str, Any]) -> str:
        """Get 'item_identifier' from item metadata entry."""
        return item_metadata["item_identifier"]
