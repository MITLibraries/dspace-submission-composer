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

    @property
    def item_identifier_column_names(self) -> list[str]:
        return ["item_identifier"]

    def get_batch_bitstream_uris(self) -> list[str]:
        return list(
            S3Client().files_iter(
                bucket=self.s3_bucket,
                prefix=self.batch_path,
                exclude_prefixes=self.exclude_prefixes,
            )
        )

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

        reconciled_items = self._match_metadata_to_bitstreams(
            metadata_item_identifiers, self.batch_bitstream_uris
        )
        self.workflow_events.reconciled_items = reconciled_items

        bitstreams_without_metadata = list(
            set(self.batch_bitstream_uris)
            - set(itertools.chain(*reconciled_items.values()))
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
                item_metadata["item_identifier"]
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

            # set column names to lowercase
            metadata_df = metadata_df.rename(columns=str.lower)

            # explicitly rename column with item identifier as 'item_identifier'
            if col_names := set(self.item_identifier_column_names).intersection(
                metadata_df.columns
            ):
                logger.warning(
                    f"Renaming multiple columns as 'item_identifier': {col_names}"
                )

            metadata_df = metadata_df.rename(
                columns={
                    col: "item_identifier"
                    for col in metadata_df.columns
                    if col in self.item_identifier_column_names
                    and col != "item_identifier"
                }
            )

            # drop any rows where all values are missing
            metadata_df = metadata_df.dropna(how="all")

            # replace all NaN values with None
            metadata_df = metadata_df.where(pd.notna(metadata_df), None)

            for _, row in metadata_df.iterrows():
                yield row.to_dict()
