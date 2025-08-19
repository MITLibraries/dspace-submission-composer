import logging
from collections.abc import Iterator
from typing import Any

import pandas as pd
import smart_open

from dsc.item_submission import ItemSubmission
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

    def get_batch_bitstreams(self) -> list[str]:
        s3_client = S3Client()
        return list(
            s3_client.files_iter(
                bucket=self.s3_bucket,
                prefix=self.batch_path,
                exclude_prefixes=self.exclude_prefixes,
            )
        )

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

    def reconcile_item(self, item_submission: ItemSubmission) -> tuple[bool, None | str]:
        """Check if any bitstreams are associated with item metadata.

        This method will match bitstreams to an item submission by filtering the
        list of URIs to those that include the item identifier as recorded
        in the metadata CSV file. If it finds any matches, the item
        submission is reconciled.
        """
        if not self.get_item_bitstreams(item_submission.item_identifier):
            return False, "missing bitstreams"
        return True, None

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
                exclude_prefixes=self.exclude_prefixes,
            )
        )
