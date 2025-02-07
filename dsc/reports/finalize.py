import csv
from io import StringIO

from dsc.reports.base import Report


class FinalizeReport(Report):
    """Report class for 'finalize' methods.

    This report is used to create an email summarizing the results
    from running the 'finalize' methods, which processes the result
    messages from DSpace Submission Service (DSS) sent to the output
    queue for a given workflow.

    The email created by this report is structured as follows:

    1. A message summarizing the number of successfully deposited items
       and the number of encountered errors.

    2. A CSV file included as an attachment describing successfully deposited items,
       which consists of the columns: 'item_identifier' and 'dspace_handle' (i.e.,
       the 'ItemHandle' from the DSS result message). Created only if
       any items in Workflow.processed_items have ingested="success".

    3. A text file included as an attachment logging all errors encountered when
       'finalize' methods were executed. Created only if any WorkflowEvents.errors exist.
    """

    @property
    def jinja_template_plain_text_filename(self) -> str:
        """Plain-text template filename."""
        return "finalize.txt"

    @property
    def jinja_template_html_filename(self) -> str:
        """HTML template filename."""
        return "finalize.html"

    @property
    def subject(self) -> str:
        return (
            f"DSpace Submission Results - {self.workflow_name}, batch='{self.batch_id}'"
        )

    def create_attachments(self) -> list[tuple]:
        """Create file attachments for 'finalize' email.

        This method will create a CSV file of successfully deposited
        items and optionally create a text file of error messages.
        """
        attachments = []

        ingested_items = self.get_ingested_items()
        if ingested_items:
            attachments.append(
                (
                    "ingested_items.csv",
                    self._write_ingested_items_csv(ingested_items),
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

    def get_ingested_items(self) -> list[dict]:
        return [
            {
                "item_identifier": item["item_identifier"],
                "dspace_handle": item["result_message"]["ItemHandle"],
            }
            for item in self.events.processed_items
            if item["ingested"] == "success"
        ]

    def _write_ingested_items_csv(self, ingested_items: list[dict]) -> StringIO:
        """Write ingested items to string buffer.

        This method creates a string buffer with the contents of a CSV
        file describing successfully ingested items.
        """
        csv_buffer = StringIO()
        fieldnames = ingested_items[0].keys()
        writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(ingested_items)
        csv_buffer.seek(0)
        return csv_buffer

    def _write_errors_text_file(self) -> StringIO:
        """Write error messages to string buffer.

        This method creates a string buffer with the error messages
        encountered when 'finalize' methods were executed.
        """
        text_buffer = StringIO()
        for error in self.events.errors:
            text_buffer.write(error + "\n")
        text_buffer.seek(0)
        return text_buffer

    def to_plain_text(self) -> str:
        return self.jinja_template_plain_text.render(
            workflow_name=self.workflow_name,
            batch_id=self.batch_id,
            report_date=self.report_date,
            processed_items=self.events.processed_items,
            errors=self.events.errors,
        )

    def to_html(self) -> str:
        return self.jinja_template_html.render(
            workflow_name=self.workflow_name,
            batch_id=self.batch_id,
            report_date=self.report_date,
            processed_items=self.events.processed_items,
            errors=self.events.errors,
        )
