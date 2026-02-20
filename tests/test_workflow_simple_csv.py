import io

import pandas as pd

from dsc.item_submission import ItemSubmission


def test_workflow_simple_csv_prepare_batch_success(
    mocked_s3_simple_csv,
    s3_client,
    simple_csv_workflow_instance,
):
    s3_client.put_file(
        file_content="",
        bucket="dsc",
        key="simple-csv/batch-aaa/123_001.pdf",
    )
    s3_client.put_file(
        file_content="",
        bucket="dsc",
        key="simple-csv/batch-aaa/123_002.pdf",
    )
    assert simple_csv_workflow_instance._prepare_batch() == (  # noqa: SLF001
        [
            ItemSubmission(
                batch_id="batch-aaa",
                item_identifier="123",
                workflow_name="simple-csv",
                dspace_metadata={
                    "dc.contributor.author": [
                        "Author 1",
                        "Author 2",
                    ],
                    "dc.date.issued": "2026",
                    "dc.title": "Title",
                },
            )
        ],
        [],
    )


def test_workflow_simple_csv_prepare_batch_track_errors(
    mocked_s3_simple_csv,
    simple_csv_workflow_instance,
):
    assert simple_csv_workflow_instance._prepare_batch() == (  # noqa: SLF001
        [],
        [("123", "No bitstreams found for the item submission")],
    )


def test_workflow_simple_csv_item_metadata_iter_success(
    simple_csv_workflow_instance, mocked_s3_simple_csv, item_submission_dspace_metadata
):
    metadata_iter = simple_csv_workflow_instance.item_metadata_iter(
        metadata_file="metadata.csv"
    )
    assert next(metadata_iter) == item_submission_dspace_metadata


def test_workflow_simple_csv_item_metadata_iter_processing_success(
    simple_csv_workflow_instance, mocked_s3
):
    metadata_df = pd.DataFrame(
        {
            "filename": ["123.pdf", "456.pdf"],
            "TITLE": ["Cheeses of the World", ""],
            "date": [2026, 2026],
            "contributor": ["Author 1", "Author 2"],
        }
    )

    # upload to mocked S3 bucket
    csv_buffer = io.StringIO()
    metadata_df.to_csv(csv_buffer, index=False)
    mocked_s3.put_object(
        Bucket="dsc",
        Key="simple-csv/batch-aaa/metadata.csv",
        Body=csv_buffer.getvalue(),
    )

    metadata_iter = simple_csv_workflow_instance.item_metadata_iter(
        metadata_file="metadata.csv"
    )
    assert list(metadata_iter) == [
        {
            "item_identifier": "123.pdf",
            "dc.title": "Cheeses of the World",
            "dc.date.issued": "2026",
            "dc.contributor.author": ["Author 1"],
        },
        {
            "item_identifier": "456.pdf",
            "dc.title": None,
            "dc.date.issued": "2026",
            "dc.contributor.author": ["Author 2"],
        },
    ]


def test_workflow_simple_csv_get_item_bitstream_uris_if_prefix_id_success(
    mocked_s3,
    simple_csv_workflow_instance,
    s3_client,
):
    s3_client.put_file(
        file_content="",
        bucket="dsc",
        key="simple-csv/batch-aaa/123_001.pdf",
    )
    s3_client.put_file(
        file_content="",
        bucket="dsc",
        key="simple-csv/batch-aaa/123_002.pdf",
    )

    assert simple_csv_workflow_instance.get_item_bitstream_uris(
        item_identifier="123"
    ) == [
        "s3://dsc/simple-csv/batch-aaa/123_001.pdf",
        "s3://dsc/simple-csv/batch-aaa/123_002.pdf",
    ]


def test_workflow_simple_csv_get_item_bitstream_uris_if_filename_id_success(
    mocked_s3,
    simple_csv_workflow_instance,
    s3_client,
):
    s3_client.put_file(
        file_content="",
        bucket="dsc",
        key="simple-csv/batch-aaa/123.pdf",
    )

    assert simple_csv_workflow_instance.get_item_bitstream_uris(
        item_identifier="123.pdf"
    ) == ["s3://dsc/simple-csv/batch-aaa/123.pdf"]
