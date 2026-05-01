# ruff: noqa: SLF001
import re

from lxml import etree

from dsc.workflows.digitized_theses import DigitizedThesesTransformer


def create_marc_source_metadata_stub(
    datafield_insert: str = "",
) -> bytes:
    """Create source record for unit tests.

    Args:
        datafield_insert (str): A string representing a MARC 'datafield' XML element.
    """
    xml_string = """<?xml version="1.0" encoding="UTF-8" standalone="no"?>
    <searchRetrieveResponse xmlns="http://www.loc.gov/zing/srw/">
        <version>1.2</version>
        <numberOfRecords>1</numberOfRecords>
        <records>
            <record>
                <recordSchema>marcxml</recordSchema>
                <recordPacking>xml</recordPacking>
                <recordData>
                    <record xmlns="http://www.loc.gov/MARC21/slim">
                        <leader>01318nam  2200373K  4500</leader>
                        {datafield_insert}
                    </record>
                </recordData>
            </record>
        </records>
    </searchRetrieveResponse>"""

    # NOTE: The regex expression removes the newline chars and whitespace, which
    # resulted from the indents added for readability, in the
    # generated XML byte string. In actual use cases, the MARC XML retrieved
    # via Alma SRU does not need this reformatting.
    source_metadata = re.sub(
        r">\s+<", "><", xml_string.format(datafield_insert=datafield_insert)
    )

    return source_metadata.encode()


def test_digitized_theses_transformer_find_record():
    source_metadata = create_marc_source_metadata_stub()
    element = DigitizedThesesTransformer._find_record(
        root=etree.fromstring(source_metadata)
    )
    assert element.tag == "{http://www.loc.gov/MARC21/slim}record"


def test_digitized_theses_transformer_datafields():
    source_metadata = create_marc_source_metadata_stub(datafield_insert="""
        <datafield ind1="4" ind2="0" tag="856">
            <subfield code="u">https://hdl.handle.net/1721.1/157046</subfield>
        </datafield>
        <datafield ind1="1" ind2="#" tag="856">
            <subfield code="u">ftp://ftp.loc.test/test.pdf</subfield>
        </datafield>""")
    element = DigitizedThesesTransformer._find_record(
        root=etree.fromstring(source_metadata)
    )
    assert (
        len(
            DigitizedThesesTransformer._datafields(
                record=element, tag=856, ind1="4", ind2="0"
            )
        )
        == 1
    )


def test_digitized_theses_transformer_datafield_text():
    source_metadata = create_marc_source_metadata_stub(datafield_insert="""
        <datafield ind1="1" ind2="0" tag="245">
            <subfield code="a">Title /</subfield>
            <subfield code="c">by Author.</subfield>
        </datafield>""")
    element = DigitizedThesesTransformer._find_record(
        root=etree.fromstring(source_metadata)
    )
    df = DigitizedThesesTransformer._datafields(record=element, tag="245")
    assert DigitizedThesesTransformer._datafield_text(df[0]) == "Title / by Author."


def test_digitized_theses_transformer_subfield_text():
    source_metadata = create_marc_source_metadata_stub(datafield_insert="""
        <datafield ind1="1" ind2="0" tag="245">
            <subfield code="a">Title /</subfield>
            <subfield code="c">by Author.</subfield>
        </datafield>""")
    element = DigitizedThesesTransformer._find_record(
        root=etree.fromstring(source_metadata)
    )
    df = DigitizedThesesTransformer._datafields(record=element, tag="245")
    assert DigitizedThesesTransformer._subfield_text(df[0], codes="a") == "Title /"


def test_digitized_theses_transformer_subfield_text_multi_codes():
    source_metadata = create_marc_source_metadata_stub(datafield_insert="""
        <datafield ind1=" " ind2="0" tag="650">
            <subfield code="a">Computer science</subfield>
            <subfield code="x">History</subfield>
        </datafield>""")
    element = DigitizedThesesTransformer._find_record(
        root=etree.fromstring(source_metadata)
    )
    df = DigitizedThesesTransformer._datafields(record=element, tag="650")
    assert (
        DigitizedThesesTransformer._subfield_text(df[0], codes="ax", separator=" | ")
        == "Computer science | History"
    )
