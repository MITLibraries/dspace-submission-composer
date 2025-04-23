from typing import Any

from dsc.workflows import SimpleCSV


class SCCS(SimpleCSV):
    """Workflow for SCCS-requested deposits.

    The deposits managed by this workflow are requested by the Scholarly
    Communication and Collection Strategy (SCCS) department
    and are for submission to DSpace@MIT.
    """

    workflow_name: str = "sccs"

    @property
    def metadata_mapping_path(self) -> str:
        return "dsc/workflows/metadata_mapping/sccs.json"

    @staticmethod
    def get_item_identifier(item_metadata: dict[str, Any]) -> str:
        """Get 'item_identifier' from item metadata entry.

        For SCCS deposits, this method expects at least one column
        labeled "item_identifier" or "filename". If both of these
        columns are present, the column labeled "item_identifier"
        is selected.
        """
        for field_name in ["item_identifier", "filename"]:
            if item_identifier := item_metadata.get(field_name):
                return item_identifier
        raise ValueError("Failed to get item identifier from source metadata")
