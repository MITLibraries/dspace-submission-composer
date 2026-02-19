import logging
from abc import abstractmethod
from collections.abc import Iterator

import numpy as np
import pandas as pd
import smart_open

from dsc.exceptions import ItemBitstreamsNotFoundError
from dsc.item_submission import ItemSubmission
from dsc.utilities.aws import S3Client
from dsc.workflows.base import Transformer, Workflow

logger = logging.getLogger(__name__)


class SimpleCSV(Workflow):
    """Base workflow for deposits that rely on a metadata CSV file.

    The metadata CSV file must be stored in a designated path for the
    deposit on S3.
    """

    workflow_name: str = "simple-csv"

    @property
    @abstractmethod
    def metadata_transformer(self) -> type[Transformer]:
        """Transformer for source metadata."""

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

    def item_metadata_iter(self, metadata_file: str = "metadata.csv") -> Iterator[dict]:
        """Yield transformed metadata from metadata CSV file.

        This method will read rows from the metadata CSV file then call
        self.metadata_transformer.transform to generate Dublin Core
        metadata for DSpace. A dict where keys = dc.element.qualifier and
        value = transformed/mapped value is returned. For logging purposes,
        the dict includes an entry for 'item_identifier'.

        If self.metadata_transformer.transform returns None (i.e.,
        something went wrong with transformation), the yielded dict will
        only include the 'item_identifier'.

        Args:
            metadata_file: Metadata CSV filename with the filename extension
                (.csv) included. Defaults to 'metadata.csv'.

        Yields:
            A dict containing the item identifier and Dublin Core metadata
            for DSpace.
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

            for _, row in metadata_df.iterrows():
                # replace all NaN values with None
                source_metadata = {
                    k: (None if isinstance(v, float) and np.isnan(v) else v)
                    for k, v in row.items()
                }

                transformed_metadata = self.metadata_transformer.transform(
                    source_metadata
                )

                if transformed_metadata:
                    yield {
                        "item_identifier": source_metadata["item_identifier"],
                        **transformed_metadata,
                    }
                else:
                    yield {
                        "item_identifier": source_metadata["item_identifier"],
                    }

    def _prepare_batch(
        self,
        *,
        synced: bool = False,  # noqa: ARG002
    ) -> tuple[list, ...]:
        """Prepare a batch of item submissions, given a metadata CSV file.

        For this workflow, the expected number of item submissions is determined
        by the number of entries in the metadata CSV file. This method
        will iterate over the rows in the metadata CSV file, using the
        provided item identifier to check if there are any matching bitstreams:

        - If no bitstreams are found, an error is recorded
        - If bitstreams are found, init params are generated for the item submission

        For SimpleCSV workflows, the batch preparation steps are the same
        for synced vs. non-synced workflows.
        """
        item_submissions = []
        errors = []

        for item_metadata in self.item_metadata_iter():
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

            # copy transformed metadata, excluding 'item_identifier'
            dspace_metadata = {
                k: v for k, v in item_metadata.items() if k != "item_identifier"
            }

            # create ItemSubmission
            item_submissions.append(
                ItemSubmission(
                    batch_id=self.batch_id,
                    item_identifier=item_metadata["item_identifier"],
                    workflow_name=self.workflow_name,
                    dspace_metadata=dspace_metadata,
                )
            )

        return item_submissions, errors
