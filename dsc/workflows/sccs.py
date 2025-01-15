from dsc.workflows import SimpleCSV


class SCCS(SimpleCSV):
    """Workflow for SCCS-requested deposits.

    The deposits managed by this workflow are requested by the Scholarly
    Communication and Collection Strategy (SCCS) department
    and are for submission to DSpace@MIT.
    """

    workflow_name: str = "sccs"
    metadata_mapping_path: str = "dsc/workflows/metadata_mapping/sccs.json"
