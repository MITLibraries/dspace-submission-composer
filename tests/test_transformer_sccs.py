from dsc.workflows.sccs import SCCSTransformer


def test_sccs_transform_filters_fields():
    source_metadata = {
        "dc.title": "A title",
        "dc.date.issued": "2026",
        "dc.contributor.author": "Author One",
        "ignored.field": "Should not be included",
    }

    assert SCCSTransformer.transform(source_metadata) == {
        "dc.title": "A title",
        "dc.date.issued": "2026",
        "dc.contributor.author": ["Author One"],
    }


def test_sccs_transform_parses_pipe_delimited_authors():
    source_metadata = {
        "dc.contributor.author": "  Author One  | |Author Two|  | Author Three ",
    }

    assert SCCSTransformer.transform(source_metadata) == {
        "dc.contributor.author": ["Author One", "Author Two", "Author Three"],
    }
