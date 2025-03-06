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

        if reconciled_items := self.events.reconciled_items:
            attachments.append(
                (
                    "reconciled_items.csv",
                    self._write_events_to_csv(
                        list(reconciled_items.items()),
                        columns=["item_identifier", "bitstreams"],
                    ),
                )
            )

        if bitstreams_without_metadata := self.events.reconcile_errors.get(
            "bitstreams_without_metadata"
        ):
            attachments.append(
                (
                    "bitstreams_without_metadata.csv",
                    self._write_events_to_csv(
                        bitstreams_without_metadata, columns=["bitstream"]
                    ),
                )
            )

        if metadata_without_bitstreams := self.events.reconcile_errors.get(
            "metadata_without_bitstreams"
        ):
            attachments.append(
                (
                    "metadata_without_bitstreams.csv",
                    self._write_events_to_csv(
                        metadata_without_bitstreams, columns=["item_identifier"]
                    ),
                )
            )
        return attachments

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
