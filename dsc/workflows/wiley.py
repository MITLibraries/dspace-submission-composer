import uuid

import smart_open

from dsc.utilities.aws import S3Client
from dsc.workflows.base import Workflow


class Wiley(Workflow):
    workflow_name: str = "wiley"

    def __init__(self, batch_id: str | None = None):
        """Initialize Wiley workflow instance.

        In the case of Wiley, an extra step is required during initialization
        when the workflow is **first** accessed for a "batch" of item submissions.
        For Wiley, a "batch" can comprise of one or more CSV files, containing the
        DOIs for MIT-affiliated authors. These CSV files are deposited into the
        workflow S3 bucket via an FTP server. To group these CSV files into a
        batch, this method will:

            - generate a 'batch_id' using uuid.uuid4() and assign it to the class
            - dynamically create a batch folder using self.batch_id
            - move the DOI CSV files into the batch folder
        """
        super().__init__(batch_id)

        # if "batch_id" is not provided, need to init batch folder
        if not batch_id:
            self.batch_id = str(uuid.uuid4())
            self._init_batch()

    def _init_batch(self) -> None:
        """Init batch folder with DOI CSV files."""

        s3_client = S3Client()
        for doi_csv_filepath in s3_client.files_iter(
            bucket=self.s3_bucket,
            prefix=self.workflow_name,
            file_type="csv",
            exclude_prefixes=self.exclude_prefixes,
        ):
            doi_csv_filename = doi_csv_filepath.split("/")[-1]
            s3_client.move_file(
                source_path=doi_csv_filepath,
                destination_path=f"s3://{self.s3_bucket}/{self.batch_path}{doi_csv_filename}",
            )

    @property
    def metadata_mapping_path(self) -> str:
        pass

    def get_batch_bitstream_uris(self):
        pass

    def item_metadata_iter(self):
        pass

    def reconcile_item(self, item_submission):
        pass
