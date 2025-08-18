import itertools
import json
import logging
from collections import defaultdict
from collections.abc import Iterator
from typing import Any

import pandas as pd
import smart_open

from dsc.db.models import ItemSubmissionStatus
from dsc.exceptions import (
    ReconcileFoundBitstreamsWithoutMetadataWarning,
    ReconcileFoundMetadataWithoutBitstreamsWarning,
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

    workflow_name: str = "simple_csv"

    @property
    def item_identifier_column_names(self) -> list[str]:
        return ["item_identifier"]

    def reconcile_items(self) -> bool:
        """Reconcile item metadata from metadata CSV file with bitstreams.

        For SimpleCSV workflows, bitstreams (files) and a metadata CSV file
        are uploaded to a designated batch folder on S3. The reconcile method
        ensures that every bitstream on S3 has metadata--a row in the metadata
        CSV file--associated with it and vice versa.
        """
        all_bitstreams = self._get_bitstreams_for_batch()

        # get bitstreams linked to item identifiers from metadata
        reconciled_items = {}
        metadata_without_bitstreams = []
        for item_metadata in self.item_metadata_iter():
            # create or get existing ItemSubmission
            item_submission = ItemSubmission.get(
                batch_id=self.batch_id, item_identifier=item_metadata["item_identifier"]
            )
            if not item_submission:
                item_submission = ItemSubmission.create(
                    batch_id=self.batch_id,
                    item_identifier=item_metadata["item_identifier"],
                    workflow_name=self.workflow_name,
                )

            if item_submission.status not in [
                None,
                ItemSubmissionStatus.RECONCILE_FAILED,
                ItemSubmissionStatus.RECONCILE_SUCCESS,
            ]:
                continue

            if item_submission.status == ItemSubmissionStatus.RECONCILE_SUCCESS:
                reconciled_items[item_submission.item_identifier] = (
                    item_submission.bitstream_s3_uris
                )
                self.reconcile_summary["reconciled"] += 1
                continue

            if bitstreams := self._get_bitstreams_for_item_submission(
                all_bitstreams, item_identifier=item_submission.item_identifier
            ):
                item_submission.status = ItemSubmissionStatus.RECONCILE_SUCCESS
                self.reconcile_summary["reconciled"] += 1
                reconciled_items[item_submission.item_identifier] = bitstreams
            else:
                item_submission.status = ItemSubmissionStatus.RECONCILE_FAILED
                metadata_without_bitstreams.append(item_submission.item_identifier)
                self.reconcile_summary["metadata_without_bitstreams"] += 1

            logger.debug(
                "Updating status for the item submission(item_identifier="
                f"{item_submission.item_identifier}): {item_submission.status}"
            )

            # save status update
            item_submission.upsert_db()

        # get bitstreams not linked to any item identifiers from metadata
        bitstreams_without_metadata = self._get_bitstreams_without_metadata(
            all_bitstreams, reconciled_items
        )
        self.reconcile_summary["bitstreams_without_metadata"] = len(
            bitstreams_without_metadata
        )

        # update WorkflowEvents with batch-level reconcile results
        self.workflow_events.reconciled_items = reconciled_items
        self.workflow_events.reconcile_errors["metadata_without_bitstreams"] = (
            metadata_without_bitstreams
        )
        self.workflow_events.reconcile_errors["bitstreams_without_metadata"] = (
            bitstreams_without_metadata
        )

        logger.info(
            f"Ran reconcile for batch '{self.batch_id}: {json.dumps(self.reconcile_summary)}"
        )

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
            return False
        else:
            logger.info(
                "Successfully reconciled bitstreams and metadata for all "
                f"{len(reconciled_items)} item(s)"
            )
            return True

    def _get_bitstreams_for_batch(self) -> list[str]:
        s3_client = S3Client()
        return list(
            s3_client.files_iter(
                bucket=self.s3_bucket,
                prefix=self.batch_path,
                exclude_prefixes=self.exclude_prefixes,
            )
        )

    @staticmethod
    def _get_bitstreams_for_item_submission(
        bitstreams: list[str], item_identifier: str
    ) -> list[str]:
        return [bitstream for bitstream in bitstreams if item_identifier in bitstream]

    @staticmethod
    def _get_bitstreams_without_metadata(
        bitstreams: list[str], reconciled_items: dict[str, list[str]]
    ) -> list[str]:
        return list(set(bitstreams) - set(itertools.chain(*reconciled_items.values())))

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
