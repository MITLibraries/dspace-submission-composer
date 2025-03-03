from dsc.reports.base import Report


class SubmitReport(Report):
    """Report class for 'submit' methods.

    This report is used to create an email summarizing the results
    from running the 'submit' methods.
    """

    @property
    def jinja_template_plain_text_filename(self) -> str:
        """Plain-text template filename."""
        return "submit.txt"

    @property
    def jinja_template_html_filename(self) -> str:
        """HTML template filename."""
        return "submit.html"

    @property
    def subject(self) -> str:
        return f"DSC Submission Results - {self.workflow_name}, batch='{self.batch_id}'"

    def create_attachments(self) -> list[tuple]:
        attachments = []

        if submitted_items := self.events.submitted_items:
            attachments.append(
                ("submitted_items.csv", self._write_events_to_csv(submitted_items))
            )

        if errors := self.events.errors:
            attachments.append(
                ("errors.csv", self._write_events_to_csv(errors, columns=["error"]))
            )
        return attachments

    def to_plain_text(self) -> str:
        return self.jinja_template_plain_text.render(
            workflow_name=self.workflow_name,
            batch_id=self.batch_id,
            report_date=self.report_date,
            submitted_items=self.events.submitted_items,
            errors=self.events.errors,
        )

    def to_html(self) -> str:
        return self.jinja_template_html.render(
            workflow_name=self.workflow_name,
            batch_id=self.batch_id,
            report_date=self.report_date,
            submitted_items=self.events.submitted_items,
            errors=self.events.errors,
        )
