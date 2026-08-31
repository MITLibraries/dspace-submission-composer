import pytest

from dsc.workflows.wiley import WileyTransformer


@pytest.fixture(autouse=True)
def wiley_source_metadata():
    """Mock source metadata for unit test.

    This mock represents a truncated version of the metadata returned by
    the Crossref API.
    """
    return {
        "publisher": "American Geophysical Union (AGU)",
        "issue": "1",
        "title": ["Awesome research title"],
        "volume": "123",
        "author": [
            {
                "ORCID": "https://orcid.org/0000-0001-2345-6789",
                "authenticated-orcid": False,
                "given": "Paige",
                "family": "Turner",
                "sequence": "first",
                "affiliation": [
                    {
                        "name": "Department of Marine Chemistry and Geochemistry Woods Hole Oceanographic Institution Woods Hole MA USA"  # noqa: E501
                    }
                ],
                "role": [{"role": "author", "vocabulary": "crossref"}],
            },
        ],
        "container-title": ["Journal of Geophysical Research: Oceans"],
        "original-title": [],
        "language": "en",
        "subtitle": [],
        "short-title": [],
        "issued": {"date-parts": [[2018, 1]]},
        "URL": "http://doi.org/10.5555/12345678",
        "ISSN": ["1234"],
        "subject": [],
        "published": {"date-parts": [[2018, 1]]},
    }


def test_wiley_transformer_dc_date_issued(wiley_source_metadata):
    assert WileyTransformer.dc_date_issued(wiley_source_metadata) == "2018-01"


def test_wiley_transformer_dc_contributor_author(wiley_source_metadata):
    assert WileyTransformer.dc_contributor_author(wiley_source_metadata) == [
        "Turner, Paige"
    ]
