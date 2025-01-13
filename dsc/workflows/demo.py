from dsc.workflows.base.simple_csv import SimpleCSV


class DemoWorkflow(SimpleCSV):

    workflow_name: str = "demo"
    submission_system: str = "DSpace@MIT"
    metadata_mapping_path: str = "tests/fixtures/demo_metadata_mapping.json"
