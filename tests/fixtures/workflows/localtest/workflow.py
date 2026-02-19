from dsc.item_submission import ItemSubmission
from dsc.workflows.base import Workflow
from dsc.workflows.opencourseware import OpenCourseWare
from dsc.workflows.simple_csv import SimpleCSV
from tests.fixtures.workflows.localtest.transformer import TestTransformer


class TestWorkflow(Workflow):

    workflow_name: str = "test"
    submission_system: str = "Test@MIT"

    @property
    def metadata_transformer(self) -> type[TestTransformer]:
        return TestTransformer

    @property
    def output_queue(self) -> str:
        return "mock-output-queue"

    def get_batch_bitstream_uris(self) -> list[str]:
        return [
            "s3://dsc/test/batch-aaa/123_01.pdf",
            "s3://dsc/test/batch-aaa/123_02.pdf",
            "s3://dsc/test/batch-aaa/789_01.pdf",
        ]

    def item_metadata_iter(self):
        yield from [
            {
                "title": "Title",
                "contributor": "Author 1|Author 2",
                "item_identifier": "123",
            },
            {
                "title": "2nd Title",
                "contributor": "Author 3|Author 4",
                "item_identifier": "789",
            },
        ]

    def _prepare_batch(self, *, synced: bool = False):  # noqa: ARG002
        return (
            [
                ItemSubmission(
                    batch_id="batch-aaa",
                    item_identifier="123",
                    workflow_name="test",
                ),
                ItemSubmission(
                    batch_id="batch-aaa",
                    item_identifier="789",
                    workflow_name="test",
                ),
            ],
            [],
        )


class TestOpenCourseWare(OpenCourseWare):

    submission_system: str = "Test@MIT"

    @property
    def output_queue(self) -> str:
        return "mock-output-queue"


class TestSimpleCSV(SimpleCSV):

    workflow_name = "simple-csv"
    submission_system: str = "Test@MIT"

    @property
    def metadata_transformer(self) -> type[TestTransformer]:
        return TestTransformer

    @property
    def item_identifier_column_names(self) -> list[str]:
        return ["item_identifier", "filename"]

    @property
    def output_queue(self) -> str:
        return "mock-output-queue"
