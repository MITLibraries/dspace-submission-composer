from __future__ import annotations

import csv
import datetime
from abc import ABC, abstractmethod
from io import StringIO
from typing import TYPE_CHECKING

from jinja2 import Environment, FileSystemLoader, select_autoescape

if TYPE_CHECKING:
    import dsc.workflows as workflows  # noqa: PLR0402


class Report(ABC):
    """A base report class from which other report classes are derived."""

    template_name: str = "base"

    def __init__(
        self, workflow_name: str, batch_id: str, events: workflows.WorkflowEvents
    ):
        self.workflow_name = workflow_name.upper()
        self.batch_id = batch_id
        self.report_date = datetime.datetime.now(tz=datetime.UTC).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        self.events = events

        # configure environment for loading jinja templates
        self.env = Environment(
            loader=FileSystemLoader(["dsc/templates/html", "dsc/templates/plain_text"]),
            autoescape=select_autoescape(),
        )

    @property
    @abstractmethod
    def subject(self) -> str:
        """Subject heading used in report email."""

    @classmethod
    def from_workflow(cls, workflow: workflows.Workflow) -> Report:
        """Create instance of Report using dsc.workflows.Workflow."""
        return cls(
            workflow_name=workflow.workflow_name.upper(),
            batch_id=workflow.batch_id,
            events=workflow.workflow_events,
        )

    @property
    @abstractmethod
    def status(self) -> str:
        """Determine the status of an executed DSC command."""

    @abstractmethod
    def create_attachments(self) -> list[tuple]:
        """Create attachments to include in report email."""

    def to_plain_text(self) -> str:
        template = self.env.get_template(f"{self.template_name}.txt")
        return template.render(
            workflow_name=self.workflow_name,
            batch_id=self.batch_id,
            report_date=self.report_date,
            status=self.status,
        )

    def to_rich_text(self) -> str:
        template = self.env.get_template(f"{self.template_name}.html")
        return template.render(
            workflow_name=self.workflow_name,
            batch_id=self.batch_id,
            report_date=self.report_date,
            status=self.status,
        )


class FinalizeReport(Report):
    """Report class for 'finalize' methods.

    This report is used to create an email summarizing the results
    from running the 'finalize' methods, which processes the result
    messages from DSpace Submission Service (DSS) sent to the output
    queue for a given workflow.

    The email created by this report contains the following:

    1. A message summarizing the number of successfully deposited items
       and the number of encountered errors.

    2. A CSV file describing successfully deposited items, which consists
       of the columns: item_identifier, doi (i.e., the "ItemHandle" from the
       DSS result message).

    3. [OPTIONAL] A text file logging all errors encountered when 'finalize'
       methods were executed.
    """

    template_name: str = "finalize"

    @property
    def subject(self) -> str:
        return (
            f"DSpace Submission Results - {self.workflow_name}, batch='{self.batch_id}'"
        )

    @property
    def status(self) -> str:
        if self.events.processed_items and not self.events.errors:
            return "success"
        if self.events.processed_items and self.events.errors:
            return "incomplete"
        return "error"

    def create_attachments(self) -> list[tuple]:
        """Create file attachments for 'finalize' email.

        This method will create a CSV file of successfully depposited
        items and optionally create a text file of error messages.
        """
        attachments = []

        if self.events.processed_items:
            attachments.append(
                (
                    "ingested_items.csv",
                    self._write_processed_items_csv(),
                )
            )

        if self.events.errors:
            attachments.append(
                (
                    "errors.txt",
                    self._write_errors_text_file(),
                )
            )
        return attachments

    def _write_processed_items_csv(self) -> StringIO:
        """Write processed items to string buffer.

        This method creates a string buffer with the contents of a CSV
        file describing successfully deposited items.
        """
        processed_items = self.events.processed_items

        csv_buffer = StringIO()
        fieldnames = processed_items[0].keys()
        writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(processed_items)
        csv_buffer.seek(0)
        return csv_buffer

    def _write_errors_text_file(self) -> StringIO:
        """Write error messages to string buffer.

        This method creates a string buffer with the error messages
        encountered when 'finalize' methods were executed.
        """
        errors = self.events.errors

        text_buffer = StringIO()
        for error in errors:
            text_buffer.write(error + "\n")
        text_buffer.seek(0)
        return text_buffer

    def to_plain_text(self) -> str:
        template = self.env.get_template(f"{self.template_name}.txt")
        return template.render(
            workflow_name=self.workflow_name,
            batch_id=self.batch_id,
            report_date=self.report_date,
            status=self.status,
            processed_items=self.events.processed_items,
            errors=self.events.errors,
        )

    def to_rich_text(self) -> str:
        template = self.env.get_template(f"{self.template_name}.html")
        return template.render(
            workflow_name=self.workflow_name,
            batch_id=self.batch_id,
            report_date=self.report_date,
            status=self.status,
            processed_items=self.events.processed_items,
            errors=self.events.errors,
        )
