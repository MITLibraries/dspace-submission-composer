import logging

import pandas as pd
import smart_open

from dsc.db.models import ItemSubmissionStatus
from dsc.item_submission import ItemSubmission
from dsc.workflows.simple_csv import SimpleCSV

logger = logging.getLogger(__name__)


class ArchivesSpace(SimpleCSV):
    """Workflow for ArchivesSpace deposits.

    The deposits managed by this workflow are requested by the Department of Distinctive
    Collections (DDC) and are submitted to Dome. This workflow utilizes the
    workflow_specific_processing() method to produce a report for linking ArchivesSpace
    records to the newly-ingested Dome items. The report is sent to an S3 bucket in the
    output_path property after it is generated.
    """

    workflow_name: str = "archivesspace"
    submission_system: str = "Dome"

    @property
    def metadata_mapping_path(self) -> str:
        return "dsc/workflows/archivesspace/metadata_mapping.json"

    @property
    def output_path(self) -> str:
        return "output-bucket"

    def workflow_specific_processing(self) -> None:
        """Generate an ingest report linking DSpace handles to ArchivesSpace URIs.

        This report is used in a separate process for updating metadata records in
        ArchivesSpace with the newly-created DSpace handles.
        """
        run_date_str = self.run_date.strftime("%Y-%m-%d-%H:%M:%S")

        handle_uri_mapping = {}

        # find item submissions that were successfully ingested on the current run
        successful_item_submissions: list[ItemSubmission] = [
            item_submission
            for item_submission in ItemSubmission.get_batch(self.batch_id)
            if item_submission.last_run_date == self.run_date
            and item_submission.status == ItemSubmissionStatus.INGEST_SUCCESS
        ]
        for item_submission in successful_item_submissions:
            handle_uri_mapping[item_submission.source_system_identifier] = (
                item_submission.dspace_handle
                if item_submission.dspace_handle
                else "DSpace handle not set, possible error"
            )
        if not handle_uri_mapping:
            logger.info(
                f"No items ingested for '{self.batch_id}' on run date '{run_date_str}'"
            )
            return

        # create dataframe
        df = pd.DataFrame(
            [
                {
                    "ao_uri": item.source_system_identifier,
                    "dspace_handle": (
                        item.dspace_handle
                        if item.dspace_handle
                        else "DSpace handle not set, possible error"
                    ),
                }
                for item in successful_item_submissions
            ]
        )

        # create report in S3 bucket
        with smart_open.open(
            f"s3://{self.output_path}/{self.batch_id}-{run_date_str}.csv", "w"
        ) as csv_file:
            df.to_csv(csv_file, index=False)

        logger.debug(
            f"Completed ingest report for batch '{self.batch_id}' on run date"
            f" '{run_date_str}'"
        )
