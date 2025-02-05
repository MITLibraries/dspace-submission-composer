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

    @property
    def s3_bucket(self) -> str:
        return "awaiting AWS infrastructure"

    @property
    def output_queue(self) -> str:
        return "awaiting AWS infrastructure"
