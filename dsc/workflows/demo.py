from dsc.workflows.base.simple_csv import SimpleCSV


class DemoWorkflow(SimpleCSV):

    workflow_name: str = "demo"
    submission_system: str = "DSpace@MIT"
    metadata_mapping_path: str = "dsc/workflows/metadata_mapping/demo.json"
