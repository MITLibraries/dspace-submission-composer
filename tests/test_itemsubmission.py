from http import HTTPStatus


def test_itemsubmission_init_success(item_submission_instance):
    assert item_submission_instance.source_metadata == {
        "title": "Title",
        "contributor": "Author 1|Author 2",
    }
    assert item_submission_instance.metadata_mapping == {
        "dc.contributor": {
            "delimiter": "|",
            "language": None,
            "source_field_name": "contributor",
        },
        "dc.title": {"delimiter": "", "language": "en_US", "source_field_name": "title"},
        "item_identifier": {
            "delimiter": "",
            "language": None,
            "source_field_name": "item_identifier",
        },
    }
    assert item_submission_instance.bitstream_uris == [
        "s3://dsc/workflow/folder/123_01.pdf",
        "s3://dsc/workflow/folder/123_02.pdf",
    ]
    assert (
        item_submission_instance.metadata_keyname == "workflow/folder/123_metadata.json"
    )


def test_generate_and_upload_dspace_metadata(
    mocked_s3, item_submission_instance, s3_client
):
    item_submission_instance.generate_and_upload_dspace_metadata("dsc")
    assert (
        item_submission_instance.metadata_uri
        == "s3://dsc/workflow/folder/123_metadata.json"
    )
    response = s3_client.client.get_object(
        Bucket="dsc", Key="workflow/folder/123_metadata.json"
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == HTTPStatus.OK


def test_create_dspace_metadata(item_submission_instance):
    assert item_submission_instance.create_dspace_metadata() == {
        "metadata": [
            {
                "key": "dc.title",
                "language": "en_US",
                "value": "Title",
            },
            {
                "key": "dc.contributor",
                "language": None,
                "value": "Author 1",
            },
            {
                "key": "dc.contributor",
                "language": None,
                "value": "Author 2",
            },
        ]
    }
