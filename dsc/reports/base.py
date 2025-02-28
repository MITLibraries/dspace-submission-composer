from __future__ import annotations

import datetime
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from jinja2 import Environment, FileSystemLoader, Template, select_autoescape

if TYPE_CHECKING:
    import dsc.workflows as workflows  # noqa: PLR0402


class Report(ABC):
    """A base report class from which other report classes are derived."""

    def __init__(
        self, workflow_name: str, batch_id: str, events: workflows.WorkflowEvents
    ):
        self.workflow_name = workflow_name
        self.batch_id = batch_id
        self.report_date = datetime.datetime.now(tz=datetime.UTC).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        self.events = events

        # configure environment for loading jinja templates
        self.jinja_env = Environment(
            loader=FileSystemLoader(
                ["dsc/reports/templates/html", "dsc/reports/templates/plain_text"]
            ),
            autoescape=select_autoescape(),
        )

    @property
    @abstractmethod
    def jinja_template_plain_text_filename(self) -> str:
        """Plain-text template filename."""

    @property
    @abstractmethod
    def jinja_template_html_filename(self) -> str:
        """HTML template filename."""

    @property
    def jinja_template_plain_text(self) -> Template:
        return self.jinja_env.get_template(self.jinja_template_plain_text_filename)

    @property
    def jinja_template_html(self) -> Template:
        return self.jinja_env.get_template(self.jinja_template_html_filename)

    @property
    @abstractmethod
    def subject(self) -> str:
        """Subject heading used in report email."""

    @classmethod
    def from_workflow(cls, workflow: workflows.Workflow) -> Report:
        """Create instance of Report using dsc.workflows.Workflow."""
        return cls(
            workflow_name=workflow.workflow_name,
            batch_id=workflow.batch_id,
            events=workflow.workflow_events,
        )

    @abstractmethod
    def create_attachments(self) -> list[tuple]:
        """Create attachments to include in report email."""

    @abstractmethod
    def to_plain_text(self) -> str:
        """Render plain-text template."""

    @abstractmethod
    def to_html(self) -> str:
        """Render HTML template."""
