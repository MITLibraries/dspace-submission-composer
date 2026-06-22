import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import UTC, datetime
from io import BytesIO, StringIO

import pandas as pd
import smart_open
from jinja2 import Environment, FileSystemLoader, Template, select_autoescape

from dsc.config import Config
from dsc.db.models import ItemSubmissionStatus
from dsc.item_submission import ItemSubmission

CONFIG = Config()
logger = logging.getLogger(__name__)


@dataclass
class Attachment:
    """File to include as part of a report.

    Attributes:
        filename: The filename to assign to attachment when included in email
        or written to a file.
        method_name: The name of a method on the Report class that retrieves
        relevant data for the attachment.
    """

    filename: str
    method_name: str


class Report(ABC):
    """Base class for all DSC reports."""

    attachments: tuple[Attachment, ...] = (
        Attachment(
            filename="item-submissions.csv", method_name="create_item_submissions_csv"
        ),
    )

    def __init__(self, workflow_name: str, batch_id: str, errors: list | None = None):
        self.workflow_name = workflow_name
        self.batch_id = batch_id
        self.errors = errors
        self.report_date = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")

        # configure environment for loading jinja templates
        self.jinja_env = Environment(
            loader=FileSystemLoader("dsc/reports/templates/"),
            autoescape=select_autoescape(),
        )

        # cache list of item submissions
        self._item_submissions: list[ItemSubmission] | None = None

    @classmethod
    def load(
        cls, workflow_name: str, batch_id: str, errors: list | None = None
    ) -> "Report":
        return cls(workflow_name=workflow_name, batch_id=batch_id, errors=errors)

    @property
    @abstractmethod
    def subject(self) -> str:
        """Subject heading for report used in email."""

    @property
    @abstractmethod
    def summary_template(self) -> Template:
        """Jinja template for report summary."""

    def get_item_submissions(self) -> list[ItemSubmission]:
        """Get item submissions for a given batch from DynamoDB."""
        if not self._item_submissions:
            self._item_submissions = list(ItemSubmission.get_batch(self.batch_id))
        return self._item_submissions

    def filter_item_submissions_by_status(self, status: str) -> list[ItemSubmission]:
        """Filter batch item submissions by status."""
        return [
            item_submission
            for item_submission in self.get_item_submissions()
            if item_submission.status == status
        ]

    @abstractmethod
    def generate_summary(self) -> str:
        """Render summary from report template.

        The generated summary will be used as the email message body.
        """

    def prepare_attachments(self) -> list[tuple]:
        """Prepare all attachments to include in report.

        For each Attachment listed in Report.attachments, this method will
        retrieve the callable referenced in Attachment.method_name() and
        run it. This method returns a list of tuples where each tuple
        contains the filename and a StringIO object (in-memory buffer).
        """
        attachments_list: list = []
        for attachment in self.attachments:
            content = getattr(self, attachment.method_name)()
            if content:
                attachments_list.append(
                    (f"{self.batch_id}-{attachment.filename}", content)
                )

        return attachments_list

    def upload_attachments(self, output_location: str) -> None:
        for filename, buffer in self.prepare_attachments():
            file = f"{output_location.removesuffix('/')}/{filename}"
            mode = "wb" if isinstance(buffer, BytesIO) else "w"
            with smart_open.open(file, mode) as f:
                f.write(buffer.getvalue())
            logger.info(f"Uploaded attachment to: {file}")

    # ====================
    # Attachment methods
    # ====================
    def create_item_submissions_csv(
        self, fields: list[str] | None = None
    ) -> StringIO | None:
        """Create a CSV from records in the DynamoDB table for a batch.

        This CSV is included in every report that is sent out to provide
        info regarding the current status of item submissions for a batch.
        """
        buffer = StringIO()
        if not fields:
            fields = [
                "batch_id",
                "item_identifier",
                "source_system_identifier",
                "status",
                "status_details",
                "dspace_handle",
                "ingest_date",
            ]

        item_submission_dicts = [
            item_submission.asdict(attrs=fields)
            for item_submission in self.get_item_submissions()
        ]
        if item_submission_dicts:
            pd.DataFrame(item_submission_dicts).to_csv(buffer, index=False)
            buffer.seek(0)
            return buffer
        return None


# =========================
# DSC step-based reports
# =========================


class CreateReport(Report):
    attachments = (
        *Report.attachments,
        Attachment(filename="errors.csv", method_name="create_errors_csv"),
    )

    @property
    def subject(self) -> str:
        return f"[{CONFIG.workspace}] DSC Create Batch Results - {self.workflow_name}, batch='{self.batch_id}'"  # noqa: E501

    @property
    def summary_template(self) -> Template:
        """Jinja template for report summary."""
        return self.jinja_env.get_template("create_summary.txt")

    def generate_summary(self) -> str:
        return self.summary_template.render(
            batch_id=self.batch_id,
            report_date=self.report_date,
            item_submissions=self.get_item_submissions(),
        )

    # ====================
    # Attachment methods
    # ====================
    def create_errors_csv(self) -> StringIO | None:
        if self.errors:
            buffer = StringIO()
            pd.DataFrame(self.errors, columns=["item_identifier", "error"]).to_csv(
                buffer, index=False
            )
            buffer.seek(0)
            return buffer
        return None


class SubmitReport(Report):
    @property
    def subject(self) -> str:
        return f"[{CONFIG.workspace}] DSC Submit Results - {self.workflow_name}, batch='{self.batch_id}'"  # noqa: E501

    @property
    def summary_template(self) -> Template:
        """Jinja template for report summary."""
        return self.jinja_env.get_template("submit_summary.txt")

    def generate_summary(self) -> str:
        return self.summary_template.render(
            batch_id=self.batch_id,
            report_date=self.report_date,
            submitted_items=self.filter_item_submissions_by_status(
                status=ItemSubmissionStatus.SUBMIT_SUCCESS
            ),
            errors=self.filter_item_submissions_by_status(
                status=ItemSubmissionStatus.SUBMIT_FAILED
            ),
        )


class FinalizeReport(Report):
    @property
    def subject(self) -> str:
        return f"[{CONFIG.workspace}] DSpace Ingest Results - {self.workflow_name}, batch='{self.batch_id}'"  # noqa: E501

    @property
    def summary_template(self) -> Template:
        """Jinja template for email summary."""
        return self.jinja_env.get_template("finalize_summary.txt")

    def generate_summary(self) -> str:
        return self.summary_template.render(
            batch_id=self.batch_id,
            report_date=self.report_date,
            ingested_items=self.filter_item_submissions_by_status(
                status=ItemSubmissionStatus.INGEST_SUCCESS
            ),
            errors=self.filter_item_submissions_by_status(
                status=ItemSubmissionStatus.INGEST_FAILED
            ),
            unknowns=self.filter_item_submissions_by_status(
                status=ItemSubmissionStatus.INGEST_UNKNOWN
            ),
        )
