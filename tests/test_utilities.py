from dsc.utilities import (
    build_bitstream_dict,
    match_bitstreams_to_item_identifiers,
    match_item_identifiers_to_bitstreams,
)


def test_build_bitstream_dict_with_file_type_success(mocked_s3, s3_client):
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/123_01.pdf")
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/123_02.pdf")
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/456_01.pdf")
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/789_01.jpg")
    assert build_bitstream_dict(
        bucket="dsc", file_type="pdf", prefix="test/batch-aaa/"
    ) == {
        "123": ["test/batch-aaa/123_01.pdf", "test/batch-aaa/123_02.pdf"],
        "456": ["test/batch-aaa/456_01.pdf"],
    }


def test_build_bitstream_dict_without_file_type_success(mocked_s3, s3_client):
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/123_01.pdf")
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/123_02.pdf")
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/456_01.pdf")
    s3_client.put_file(file_content="", bucket="dsc", key="test/batch-aaa/789_01.jpg")
    assert build_bitstream_dict(bucket="dsc", file_type="", prefix="test/batch-aaa/") == {
        "123": ["test/batch-aaa/123_01.pdf", "test/batch-aaa/123_02.pdf"],
        "456": ["test/batch-aaa/456_01.pdf"],
        "789": ["test/batch-aaa/789_01.jpg"],
    }


def test_match_item_identifiers_to_bitstreams_success():
    bitstream_dict = {"test": "test_01.pdf"}
    item_identifiers = ["test", "tast"]
    item_identifier_matches = match_item_identifiers_to_bitstreams(
        bitstream_dict.keys(), item_identifiers
    )
    assert len(item_identifier_matches) == 1
    assert "test" in item_identifier_matches


def test_match_bitstreams_to_item_identifiers_success():
    bitstream_dict = {"test": "test_01.pdf", "tast": "tast_01.pdf"}
    item_identifiers = ["test"]
    file_matches = match_bitstreams_to_item_identifiers(bitstream_dict, item_identifiers)
    assert len(file_matches) == 1
    assert "test" in file_matches
