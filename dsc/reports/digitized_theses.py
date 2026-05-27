from io import BytesIO, StringIO
import pandas as pd
from lxml import etree

from dsc.db.models import ItemSubmissionStatus
from dsc.reports.base import Attachment, FinalizeReport


class DigitizedThesesFinalizeReport(FinalizeReport):

    attachments = (
        *FinalizeReport.attachments,
        Attachment(
            filename="FM_DSpaceURLS_Tab.txt", method_name="create_filemaker_export_text"
        ),
        Attachment(
            filename="ALMA_DSpaceURLS_Tab.xml", method_name="create_alma_export_xml"
        ),
    )

    # ====================
    # Attachment methods
    # ====================

    def create_filemaker_export_text(self) -> StringIO:
        """Create tab-delimited text file with OCLC numbers and DSpace handles."""
        buffer = StringIO()
        fields = ["item_identifier", "dspace_handle"]

        # get items successfully ingested into DSpace
        item_submission_dicts = [
            item_submission.asdict(attrs=fields)
            for item_submission in self.get_item_submissions()
            if item_submission.status == ItemSubmissionStatus.INGEST_SUCCESS
        ]
        pd.DataFrame(item_submission_dicts).to_csv(
            buffer, sep="\t", header=False, index=False
        )
        buffer.seek(0)

        return buffer

    def create_alma_export_xml(self) -> BytesIO:
        """Create an XML file with OCLC numbers and DSpace handles."""
        buffer = BytesIO()
        fields = ["item_identifier", "dspace_handle"]

        # get items successfully ingested into DSpace
        item_submission_dicts = [
            item_submission.asdict(attrs=fields)
            for item_submission in self.get_item_submissions()
            if item_submission.status == ItemSubmissionStatus.INGEST_SUCCESS
        ]

        root = etree.Element("collection")
        for item_submission in item_submission_dicts:
            record = etree.SubElement(root, "record")

            # write OCoLC (item identifier) to MARC 035 $a
            system_control_number = self._create_datafield_element(
                text=item_submission["item_identifier"],
                tag="035",
                subfield_code="a",
                ind1=" ",
                ind2=" ",
            )
            record.append(system_control_number)

            # write DSpace handle to MARC 856 $u
            uniform_resource_identifier = self._create_datafield_element(
                text=item_submission["dspace_handle"],
                tag="856",
                subfield_code="u",
                ind1="4",
                ind2="0",
            )
            record.append(uniform_resource_identifier)

        tree = etree.ElementTree(root)
        tree.write(buffer, encoding="utf-8", xml_declaration=True)
        buffer.seek(0)

        return buffer

    @staticmethod
    def _create_datafield_element(
        text: str, tag: str, subfield_code: str, **attrs: str
    ) -> etree._Element:
        datafield = etree.Element("datafield", tag=tag, **attrs)
        child = etree.SubElement(datafield, "subfield", code=subfield_code)
        child.text = text

        return datafield
