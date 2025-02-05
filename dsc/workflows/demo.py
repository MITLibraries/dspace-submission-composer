from dsc.workflows.base.simple_csv import SimpleCSV


class Demo(SimpleCSV):

    workflow_name: str = "demo"
    submission_system: str = "DSpace@MIT"

    @property
    def metadata_mapping_path(self) -> str:
        return "dsc/workflows/metadata_mapping/demo.json"

    @property
    def s3_bucket(self) -> str:
        return "wiley-files-dev-222053980223"

    @property
    def output_queue(self) -> str:
        return "dss-wiley-output-dev"
