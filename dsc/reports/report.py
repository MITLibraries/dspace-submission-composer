from __future__ import annotations

import datetime
import logging
from abc import ABC, abstractmethod
from io import StringIO
from typing import TYPE_CHECKING, ClassVar

import boto3
import pandas as pd
from jinja2 import Environment, FileSystemLoader, Template, select_autoescape

from dsc.config import Config
from dsc.db.models import ItemSubmissionStatus
from dsc.item_submission import ItemSubmission

if TYPE_CHECKING:
    from pathlib import Path

    from dsc import workflows

logger = logging.getLogger(__name__)
CONFIG = Config()


class Report(ABC):
    """Base class for all DSC reporting."""

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
        self._item_submissions: list[ItemSubmission] | None = None

    @property
    @abstractmethod
    def subject(self) -> str:
        """Subject heading for report used in email."""

    @property
    @abstractmethod
    def summary_template(self) -> Template:
        """Jinja template for report summary."""

    @abstractmethod
    def generate_summary(self) -> str:
        """Render summary from report template.

        The generated summary will be used as the email message body.
        """

    def get_batch_item_submissions(self) -> list[ItemSubmission]:
        """Get item submissions for a given batch from DynamoDB.

        Retrieved data is cached to self._item_submissions.
        """
        if not self._item_submissions:
            self._item_submissions = list(ItemSubmission.get_batch(self.batch_id))
        return self._item_submissions

    @classmethod
    def from_workflow(cls, workflow: workflows.Workflow) -> Report:
        """Create instance of Report using dsc.workflows.Workflow."""
        return cls(
            workflow_name=workflow.workflow_name,
            batch_id=workflow.batch_id,
        )

    def write_item_submissions_csv(
        self, output_file: StringIO | str | Path
    ) -> StringIO | str | Path:
        """Write report data to either a CSV file or in-memory string buffer."""
        if isinstance(output_file, StringIO):
            logger.debug("Writing data to CSV buffer")
        else:
            logger.debug("Writing data to CSV file: %s", output_file)

        batch_item_submissions_dict = [
            item_submission.asdict(attrs=self.fields)
            for item_submission in self.get_batch_item_submissions()
        ]

        pd.DataFrame(batch_item_submissions_dict).to_csv(output_file, index=False)
        return output_file

    def _filter_item_submissions_by_status(self, status: str) -> list[ItemSubmission]:
        """Filter batch item submissions by status."""
        return [
            item_submission
            for item_submission in self.get_batch_item_submissions()
            if item_submission.status == status
        ]


class CreateReport(Report):

    @property
    def subject(self) -> str:
        return f"DSC Create Batch Results - {self.workflow_name}, batch='{self.batch_id}'"

    @property
    def summary_template(self) -> Template:
        """Jinja template for report summary."""
        return self.jinja_env.get_template("create_summary.txt")

    def generate_summary(self) -> str:
        return self.summary_template.render(
            batch_id=self.batch_id,
            report_date=self.report_date,
            item_submissions=self.get_batch_item_submissions(),
        )


class SubmitReport(Report):

    @property
    def subject(self) -> str:
        return f"DSC Submit Results - {self.workflow_name}, batch='{self.batch_id}'"

    @property
    def summary_template(self) -> Template:
        """Jinja template for report summary."""
        return self.jinja_env.get_template("submit_summary.txt")

    def generate_summary(self) -> str:
        return self.summary_template.render(
            batch_id=self.batch_id,
            report_date=self.report_date,
            submitted_items=self._filter_item_submissions_by_status(
                status=ItemSubmissionStatus.SUBMIT_SUCCESS
            ),
            errors=self._filter_item_submissions_by_status(
                status=ItemSubmissionStatus.SUBMIT_FAILED
            ),
        )


class FinalizeReport(Report):
    @property
    def subject(self) -> str:
        return (
            f"DSpace Submission Results - {self.workflow_name}, batch='{self.batch_id}'"
        )

    @property
    def summary_template(self) -> Template:
        """Jinja template for report summary."""
        return self.jinja_env.get_template("finalize_summary.txt")

    def generate_summary(self) -> str:
        return self.summary_template.render(
            batch_id=self.batch_id,
            report_date=self.report_date,
            ingested_items=self._filter_item_submissions_by_status(
                status=ItemSubmissionStatus.INGEST_SUCCESS
            ),
            errors=self._filter_item_submissions_by_status(
                status=ItemSubmissionStatus.INGEST_FAILED
            ),
            unknowns=self._filter_item_submissions_by_status(
                status=ItemSubmissionStatus.INGEST_UNKNOWN
            ),
        )
