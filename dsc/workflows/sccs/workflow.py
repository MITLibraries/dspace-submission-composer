from dsc.workflows.simple_csv import SimpleCSV


class SCCS(SimpleCSV):
    """Workflow for SCCS-requested deposits.

    The deposits managed by this workflow are requested by the Scholarly
    Communication and Collection Strategy (SCCS) department
    and are for submission to DSpace@MIT.
    """

    workflow_name: str = "sccs"

    @property
    def metadata_mapping_path(self) -> str:
        return "dsc/workflows/sccs/metadata_mapping.json"

    @property
    def item_identifier_column_names(self) -> list[str]:
        return ["item_identifier", "filename"]
