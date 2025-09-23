import logging
from collections.abc import Iterator
from typing import Any

import pandas as pd
import smart_open

from dsc.exceptions import (
    ItemBitstreamsNotFoundError,
    ItemMetadataNotFoundError,
    ReconcileFailedMissingBitstreamsError,
)
from dsc.item_submission import ItemSubmission
from dsc.utilities.aws import S3Client
from dsc.workflows.base import Workflow

logger = logging.getLogger(__name__)


class SimpleCSV(Workflow):
    """Base workflow for deposits that rely on a metadata CSV file.

    The metadata CSV file must be stored in a designated path for the
    deposit on S3.
    """

    workflow_name: str = "simple-csv"

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

    def reconcile_item(self, item_submission: ItemSubmission) -> bool:
        """Check whether ItemSubmission is associated with any bitstreams.

        This method will match bitstreams to an item submission by filtering the
        list of URIs to those that include the item identifier as recorded
        in the metadata CSV file. If it finds any matches, the item
        submission is reconciled.
        """
        if not self.get_item_bitstream_uris(item_submission.item_identifier):
            raise ReconcileFailedMissingBitstreamsError
        return True

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

    def prepare_batch(self) -> tuple[list, ...]:
        item_submission_init_params = []
        errors = []

        for item_metadata in self.item_metadata_iter():
            # check if there is sufficient metadata for the item submission
            if self._insufficient_item_metadata(item_metadata):
                errors.append(
                    (
                        item_metadata["item_identifier"],
                        str(ItemMetadataNotFoundError),
                    )
                )
                continue

            # check if there are any bitstreams associated with the item submission
            if not self.get_item_bitstream_uris(
                item_identifier=item_metadata["item_identifier"]
            ):
                errors.append(
                    (item_metadata["item_identifier"], str(ItemBitstreamsNotFoundError))
                )
                continue

            # if item submission has sufficient metadata and bitstreams
            # save init params
            item_submission_init_params.append(
                {
                    "batch_id": self.batch_id,
                    "item_identifier": item_metadata["item_identifier"],
                    "workflow_name": self.workflow_name,
                }
            )
        return item_submission_init_params, errors

    def _insufficient_item_metadata(self, item_metadata: dict) -> bool:
        """Check if the metadata for the item is sufficient.

        All item metadata will have a set value for the 'item_identifier' field.
        If the count of non-null fields is exactly 1, this means only the item
        identifier has been provided; therefore the metadata is insufficient.
        """
        return sum(1 for value in item_metadata.values() if value is not None) == 1
