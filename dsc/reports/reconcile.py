from dsc.reports.base import Report


class ReconcileReport(Report):
    """Report class for 'reconcile' methods."""

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
