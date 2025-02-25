from dsc.workflows import SCCS


def test_workflow_sccs_get_item_identifier_success(item_metadata):
    assert SCCS.get_item_identifier(item_metadata) == "123"


def test_workflow_sccs_get_item_identifier_if_filename_success(item_metadata):
    item_metadata.pop("item_identifier")
    item_metadata["filename"] = "123.pdf"

    assert SCCS.get_item_identifier(item_metadata) == "123.pdf"


def test_workflow_sccs_get_item_identifier_if_multi_fields_success(item_metadata):
    item_metadata["filename"] = "123.pdf"

    assert SCCS.get_item_identifier(item_metadata) == "123"
