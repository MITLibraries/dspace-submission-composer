import csv
import logging
from typing import Any, Iterator

import smart_open

from dsc.workflows.base import BaseWorkflow
from dsc.utilities.aws import S3Client

logger = logging.getLogger(__name__)


class SimpleCSV(BaseWorkflow):
    def batch_metadata_iter(
        self, metadata_file: str = "metadata.csv"
    ) -> Iterator[dict[str, Any]]:
        """Yield dicts of item metadata from metadata CSV file.

        Args:
            metadata_file (str, optional): Metadata CSV filename with
                the filename extension (.csv) included.
                Defaults to 'metadata.csv'.

        Yields:
            Iterator[dict[str, Any]]: Item metadata.
        """
        with smart_open.open(
            f"s3://{self.s3_bucket}/{self.s3_prefix}/{metadata_file}"
        ) as csvfile:
            reader = csv.DictReader(csvfile)
            for metadata_entry in reader:
                yield metadata_entry

    def get_bitstream_uris(self, item_identifier: str) -> list[str]:
        """Get S3 URIs for bitstreams for a given item.

        This method uses S3Client.get_files_iter to get a list of files
        on S3 stored at s3://bucket/prefix/ and includes the 'item_identifier'
        in the object key.

        - If the exact filename is provided as 'item_identifier', only
          one bitstream is retrieved.
        - If a prefix is provided as 'item_identifier', one or more
          bitstreams are retrieved.

        Args:
            item_identifier (str): Item identifier used to filter bitstreams.

        Returns:
            list[str]: Bitstream URIs for a given item.
        """
        s3_client = S3Client()
        return [
            bitstream_uri
            for bitstream_uri in s3_client.get_files_iter(
                bucket=self.s3_bucket,
                prefix=self.s3_prefix,
                file_identifier=item_identifier,
            )
        ]

    def get_item_identifier(self, item_metadata: dict[str, Any]) -> str:
        """Get 'item_identifier' from item metadata entry."""
        return item_metadata["item_identifier"]

    def process_deposit_results(self):
        pass
