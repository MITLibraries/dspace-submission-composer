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

        attachments.append(
            (
                "dss_submission_results.csv",
                self._write_events_to_csv(self.events.processed_items),
            )
        )
        return attachments

    def create_summary(self) -> dict:
        return {
            "processed": len(self.events.processed_items),
            "ingested": sum(
                1
                for processed_item in self.events.processed_items
                if processed_item.get("ingested")
            ),
            "error": sum(
                1
                for processed_item in self.events.processed_items
                if processed_item.get("error")
            ),
        }

    def to_plain_text(self) -> str:
        return self.jinja_template_plain_text.render(
            workflow_name=self.workflow_name,
            batch_id=self.batch_id,
            report_date=self.report_date,
            summary=self.create_summary(),
        )

    def to_html(self) -> str:
        return self.jinja_template_html.render(
            workflow_name=self.workflow_name,
            batch_id=self.batch_id,
            report_date=self.report_date,
            summary=self.create_summary(),
        )
