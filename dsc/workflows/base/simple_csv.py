import itertools
import json
import logging
from collections import defaultdict
from collections.abc import Iterator
from typing import Any

import pandas as pd
import smart_open

from dsc.exceptions import (
    ReconcileFoundBitstreamsWithoutMetadataWarning,
    ReconcileFoundMetadataWithoutBitstreamsWarning,
)
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
    ) -> bool:
        """Reconcile item metadata from metadata CSV file with bitstreams.

        For SimpleCSV workflows, bitstreams (files) and a metadata CSV file
        are uploaded to a designated batch folder on S3. The reconcile method
        ensures that every bitstream on S3 has metadata--a row in the metadata
        CSV file--associated with it and vice versa.
        """
        logger.info(f"Reconciling bitstreams and metadata for batch '{self.batch_id}'")
        reconciled: bool = False
        reconcile_summary = {
            "reconciled": 0,
            "bitstreams_without_metadata": 0,
            "metadata_without_bitstreams": 0,
        }

        # get metadata
        metadata_item_identifiers = self._get_item_identifiers_from_metadata(
            metadata_file
        )

        # get bitstreams
        s3_client = S3Client()
        bitstream_filenames = list(
            s3_client.files_iter(
                bucket=self.s3_bucket,
                prefix=self.batch_path,
                exclude_prefixes=["archived", metadata_file, "metadata.json"],
            )
        )

        reconciled_items = self._match_metadata_to_bitstreams(
            metadata_item_identifiers, bitstream_filenames
        )
        self.workflow_events.reconciled_items = reconciled_items

        bitstreams_without_metadata = list(
            set(bitstream_filenames) - set(itertools.chain(*reconciled_items.values()))
        )
        metadata_without_bitstreams = list(
            metadata_item_identifiers - set(reconciled_items.keys())
        )
        reconcile_summary.update(
            {
                "reconciled": len(reconciled_items),
                "bitstreams_without_metadata": len(bitstreams_without_metadata),
                "metadata_without_bitstreams": len(metadata_without_bitstreams),
            }
        )
        logger.info(f"Reconcile results: {json.dumps(reconcile_summary)}")

        if any((bitstreams_without_metadata, metadata_without_bitstreams)):
            logger.warning("Failed to reconcile bitstreams and metadata")

            if bitstreams_without_metadata:
                logger.warning(
                    ReconcileFoundBitstreamsWithoutMetadataWarning(
                        bitstreams_without_metadata
                    )
                )
                self.workflow_events.reconcile_errors["bitstreams_without_metadata"] = (
                    bitstreams_without_metadata
                )

            if metadata_without_bitstreams:
                logger.warning(
                    ReconcileFoundMetadataWithoutBitstreamsWarning(
                        metadata_without_bitstreams
                    )
                )
                self.workflow_events.reconcile_errors["metadata_without_bitstreams"] = (
                    metadata_without_bitstreams
                )
        else:
            reconciled = True
            logger.info(
                "Successfully reconciled bitstreams and metadata for all "
                f"{len(reconciled_items)} item(s)"
            )

        return reconciled

    def _match_metadata_to_bitstreams(
        self, item_identifiers: set[str], bitstream_filenames: list[str]
    ) -> dict:
        metadata_with_bitstreams = defaultdict(list)
        for item_identifier in item_identifiers:
            for bitstream_filename in bitstream_filenames:
                if item_identifier in bitstream_filename:
                    metadata_with_bitstreams[item_identifier].append(bitstream_filename)
        return metadata_with_bitstreams

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
            f"s3://{self.s3_bucket}/{self.batch_path}{metadata_file}",
        ) as csvfile:
            metadata_df = pd.read_csv(csvfile, dtype="str")
            metadata_df = metadata_df.dropna(how="all")
            metadata_df = metadata_df.where(pd.notna(metadata_df), None)

            for _, row in metadata_df.iterrows():
                yield row.to_dict()

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
                exclude_prefixes=["archived", "metadata.json"],
            )
        )

    @staticmethod
    def get_item_identifier(item_metadata: dict[str, Any]) -> str:
        """Get 'item_identifier' from item metadata entry.

        This method expects a column labeled 'item_identifier' in the
        source metadata file.
        """
        return item_metadata["item_identifier"]
