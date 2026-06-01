"""DSC reporting module.

This module provides a framework for generating workflow-specific reports at different
stages of the DSC pipeline. Reports are used to communicate batch results to
stakeholders via email, capturing status information and detailed metrics for
each workflow execution.

A single Report refers to the (1) email, (2) the summary in the email message body,
and (3) file attachments.

## Report Types

The module defines three main report classes that correspond to key workflow stages:

- **CreateReport**: Generated after batch creation. Provides an overview of items
  created in DynamoDB. Intended for DSC managers.
- **SubmitReport**: Generated after queueing a batch for ingest. Provides an overview
  of submission messages sent (or failed to send) to the DSS. Intended for DSC managers.
- **FinalizeReport**: Generated after attempting DSpace ingest. Reports on
  successfully ingested items, ingest failures, and items with unknown status.
  Intended for both DSC managers and users.

All report classes inherit from the abstract base `Report` class, which provides common
functionality for managing batch data, rendering templates, and handling attachments.

## Attachments

Attachments are files included with each report to provide supporting data and metadata.
The attachment system is flexible and extensible, allowing workflows to customize which
files are attached to each report type.

### How Attachments Work

Attachments are defined as instances of the `Attachment` dataclass, which specifies:
- **filename**: The name of the file in the report (e.g., "item-submissions.csv").
- **method_name**: The name of a method on the Report class that generates the
  attachment content.

Attachments are registered by setting the `attachments` class attribute on a Report
class to a tuple of `Attachment` instances. When a report is generated, the
`prepare_attachments()` method iterates through this tuple, calls each method by name,
and returns a list of (filename, StringIO) tuples ready for delivery.

### Creating Custom Attachments

To add custom attachments for a workflow, follow these steps:

1. **Define an attachment method** on your Report subclass that returns either a
   BytesIO or StringIO object containing the attachment data:

   ```python
   def create_custom_data(self) -> StringIO:
       \"\"\"Generate custom attachment content.\"\"\"
       buffer = StringIO()
       # Generate your data and write to buffer
       buffer.seek(0)
       return buffer
   ```

2. **Register the attachment** in the class definition by setting or extending the
   `attachments` tuple:

   ```python
   class MyCustomReport(FinalizeReport):
       attachments = (
           *FinalizeReport.attachments,  # Include parent attachments
           Attachment(
               filename="custom-data.txt",
               method_name="create_custom_data"
           ),
       )
   ```

3. **Implement the method** in your Report subclass with the logic to generate the
   attachment content.

### Example

The `DigitizedThesesFinalizeReport` demonstrates a custom attachment implementation:

```python
from dsc.reports.base import Attachment, FinalizeReport

class DigitizedThesesFinalizeReport(FinalizeReport):
    attachments = (
        *FinalizeReport.attachments,
        Attachment(filename="FM_DSpaceURLS_Tab.txt",
                   method_name="create_filemaker_export_text"),
        Attachment(filename="ALMA_DSpaceURLS_Tab.xml",
                   method_name="create_alma_export_xml"),
    )

    def create_filemaker_export_text(self) -> StringIO:
        # Implementation that generates FileMaker-compatible export
        ...

    def create_alma_export_xml(self) -> StringIO:
        # Implementation that generates ALMA-compatible XML export
        ...
```

This pattern allows each workflow to define and attach exactly the files needed for
its downstream systems and stakeholders.
"""

from dsc.reports.base import CreateReport, FinalizeReport, Report, SubmitReport
from dsc.reports.digitized_theses import DigitizedThesesFinalizeReport

__all__ = [
    "CreateReport",
    "DigitizedThesesFinalizeReport",
    "FinalizeReport",
    "Report",
    "SubmitReport",
]
