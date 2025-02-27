from io import StringIO
from typing import Literal

from dsc.reports.base import Report


class ReconcileReport(Report):
    """Report class for 'reconcile' methods.

    This report is used to create an email summarizing the results
    from running the 'reconcile' methods.

    The email created by this report is structured as follows:

    1. A message summarizing the number of successfully reconciled items
       and the number of reconcile errors.

    2. A text file included as an attachment describing reconciled items.

    3. Text file(s) included as attachment(s) describing reconcile errors.
    """

    @property
    def jinja_template_plain_text_filename(self) -> str:
        """Plain-text template filename."""
        return "reconcile.txt"

    @property
    def jinja_template_html_filename(self) -> str:
        """HTML template filename."""
        return "reconcile.html"

    @property
    def subject(self) -> str:
        return f"DSC Reconcile Results - {self.workflow_name}, batch='{self.batch_id}'"

    def create_attachments(self) -> list[tuple]:
        """Create file attachments for 'reconcile' email.

        This method will create text files describing reconciled items
        and reconcile errors.
        """
        attachments = []

        if self.events.reconciled_items:
            attachments.append(
                ("reconciled_items.txt", self._write_reconciled_items_text())
            )

        if self.events.reconcile_errors.get("bitstreams_without_metadata"):
            attachments.append(
                (
                    "bitstreams_without_metadata.txt",
                    self._write_reconcile_errors_text(
                        error_type="bitstreams_without_metadata",
                        header="Bitstreams without metadata",
                    ),
                )
            )

        if self.events.reconcile_errors.get("metadata_without_bitstreams"):
            attachments.append(
                (
                    "metadata_without_bitstreams.txt",
                    self._write_reconcile_errors_text(
                        error_type="metadata_without_bitstreams",
                        header="Metadata without bitstreams",
                    ),
                )
            )
        return attachments

    def _write_reconciled_items_text(self) -> StringIO:
        """Write reconciled items to string buffer.

        This method creates a string buffer containing an enumerated list of
        reconciled items formatted as:
            "<item_identifier> -> [<bitstream filenames>]'
        """
        text_buffer = StringIO()
        text_buffer.write("Reconciled items (item_identifier -> bitstreams)\n\n")

        for index, (item_identifier, bitstreams) in enumerate(
            self.events.reconciled_items.items(), start=1
        ):
            text_buffer.write(f"{index}. {item_identifier} -> {bitstreams}\n")
        text_buffer.seek(0)

        return text_buffer

    def _write_reconcile_errors_text(
        self,
        error_type: Literal["bitstreams_without_metadata", "metadata_without_bitstreams"],
        header: str,
    ) -> StringIO:
        """Write reconcile errors to string buffer.

        This method creates a string buffer containing an enumerated list of
        reconcile errors -- either item identifiers for 'metadata_without_bitstreams'
        or bitstream filenames for 'bitstreams_without_metadata'.
        """
        text_buffer = StringIO()
        text_buffer.write(f"{header}\n\n")

        for index, reconcile_error in enumerate(
            self.events.reconcile_errors[error_type], start=1
        ):
            text_buffer.write(f"{index}. {reconcile_error}\n")
        text_buffer.seek(0)

        return text_buffer

    def to_plain_text(self) -> str:
        return self.jinja_template_plain_text.render(
            workflow_name=self.workflow_name,
            batch_id=self.batch_id,
            report_date=self.report_date,
            reconciled_items=self.events.reconciled_items,
            bitstreams_without_metadata=self.events.reconcile_errors.get(
                "bitstreams_without_metadata"
            ),
            metadata_without_bitstreams=self.events.reconcile_errors.get(
                "metadata_without_bitstreams"
            ),
        )

    def to_html(self) -> str:
        return self.jinja_template_html.render(
            workflow_name=self.workflow_name,
            batch_id=self.batch_id,
            report_date=self.report_date,
            processed_items=self.events.processed_items,
            bitstreams_without_metadata=self.events.reconcile_errors.get(
                "bitstreams_without_metadata"
            ),
            metadata_without_bitstreams=self.events.reconcile_errors.get(
                "metadata_without_bitstreams"
            ),
        )
