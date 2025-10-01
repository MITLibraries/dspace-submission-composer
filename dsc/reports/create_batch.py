from __future__ import annotations

import datetime
import logging
from io import StringIO
from typing import TYPE_CHECKING, ClassVar

import boto3
import pandas as pd
from jinja2 import Environment, FileSystemLoader, Template, select_autoescape

from dsc.config import Config
from dsc.item_submission import ItemSubmission

if TYPE_CHECKING:
    from pathlib import Path

    from dsc import workflows

logger = logging.getLogger(__name__)
CONFIG = Config()


class CreateBatchReport:
    """Report class for Workflow.create_batch method.

    The email created by this report is structured as follows:

    1. A message summarizing the number of successfully created item
       submissions in DynamoDB.
    2. A CSV file containing all item submissions for a given batch
       that were written to DynamoDB.
    """

    fields: ClassVar[list[str]] = [
        "batch_id",
        "item_identifier",
        "source_system_identifier",
        "status",
        "status_details",
        "dspace_handle",
        "ingest_date",
    ]

    def __init__(self, workflow_name: str, batch_id: str):
        self.workflow_name = workflow_name
        self.batch_id = batch_id
        self.client = boto3.client("dynamodb")
        self.report_date = datetime.datetime.now(tz=datetime.UTC).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        # configure environment for loading jinja templates
        self.jinja_env = Environment(
            loader=FileSystemLoader("dsc/reports/templates/"),
            autoescape=select_autoescape(),
        )

        # cache list of item submissions
        self._item_submissions: list[dict] | None = None

    @property
    def summary_template(self) -> Template:
        """Jinja template for report summary."""
        return self.jinja_env.get_template("create_batch_report.txt")

    @property
    def subject(self) -> str:
        """Subject heading for report used ijn email."""
        return f"DSC Create Batch Results - {self.workflow_name}, batch='{self.batch_id}'"

    @property
    def item_submissions(self) -> list[dict]:
        """Report data retrieved from DynamoDB.

        An effect of running Workflow.create_batch is the writing of item submissions
        to DynamoDB. Therefore, this report reads the following data from DynamoDB:

        - All item submissions for a given batch
        """
        if not self._item_submissions:
            self._item_submissions = [
                item_submission.asdict_subset(fields=self.fields)
                for item_submission in ItemSubmission.get_batch(self.batch_id)
            ]
        return self._item_submissions

    @classmethod
    def from_workflow(cls, workflow: workflows.Workflow) -> CreateBatchReport:
        """Create instance of Report using dsc.workflows.Workflow."""
        return cls(
            workflow_name=workflow.workflow_name,
            batch_id=workflow.batch_id,
        )

    def generate_summary(self) -> str:
        """Render summary from report template.

        The generated summary will be used as the email message body.
        """
        return self.summary_template.render(
            batch_id=self.batch_id,
            report_date=self.report_date,
            item_submissions=self.item_submissions,
        )

    def create_email_attachments(self) -> list[tuple]:
        """Create attachments to include in report email."""
        return [("create_batch_results.csv", self.write_to_csv(output_file=StringIO()))]

    def write_to_csv(self, output_file: StringIO | str | Path) -> StringIO | str | Path:
        """Write report data to either a CSV file or in-memory string buffer."""
        if isinstance(output_file, StringIO):
            logger.debug("Writing data to CSV buffer")
        else:
            logger.debug("Writing data to CSV file: %s", output_file)
        pd.DataFrame(self.item_submissions).to_csv(output_file, index=False)
        return output_file
