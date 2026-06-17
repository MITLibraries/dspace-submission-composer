from __future__ import annotations

import inspect
import re
from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from collections.abc import Iterable

from lxml import etree

from dsc.config import load_external_config

NSMAP = {"marc": "http://www.loc.gov/MARC21/slim", "sru": "http://www.loc.gov/zing/srw/"}


class DigitizedThesesTransformer:
    """Transformer for Digitized Theses source metadata.

    The transformer expects a MARC XML record in byte-string form as its input
    and returns a dictionary with Qualified Dublin Core metadata. The field methods
    are based on the transformations in the  MIT-customized 'marc21-to-dc.xsl'
    stylesheet, which itself is based on the Library of Congress MARC-to-DC
    crosswalk. The transformer includes crosswalks to normalize values
    for select fields (dc.contributor.department and mit.thesis.degree).
    """

    fields: Iterable[str] = [
        # fields with derived values
        "dc_title",
        "dc_date_issued",
        "dc_contributor_advisor",
        "dc_contributor_author",
        "dc_contributor_department",
        "dc_contributor_other",
        "dc_coverage_spatial",
        "dc_coverage_temporal",
        "dc_date_copyright",
        "dc_date_created",
        "dc_description",
        "dc_description_abstract",
        "dc_description_collection",
        "dc_description_degree",
        "dc_description_provenance",
        "dc_description_sponsorship",
        "dc_description_statementofresponsibility",
        "dc_description_tableofcontents",
        "dc_format",
        "dc_format_extent",
        "dc_format_medium",
        "dc_identifier_govdoc",
        "dc_identifier_isbn",
        "dc_identifier_issn",
        "dc_identifier_ismn",
        "dc_identifier_other",
        "dc_identifier_sici",
        "dc_identifier_uri",
        "dc_language",
        "dc_language_iso",
        "dc_publisher",
        "dc_relation",
        "dc_relation_haspart",
        "dc_relation_isbasedon",
        "dc_relation_isformatof",
        "dc_relation_ispartof",
        "dc_relation_ispartofseries",
        "dc_relation_isreferencedby",
        "dc_relation_isreplacedby",
        "dc_relation_replaces",
        "dc_relation_requires",
        "dc_relation_uri",
        "dc_rights",
        "dc_rights_uri",
        "dc_source",
        "dc_subject",
        "dc_subject_ddc",
        "dc_subject_lcc",
        "dc_subject_lcsh",
        "dc_subject_mesh",
        "dc_subject_other",
        "dc_title_alternative",
        "dc_type",
        "mit_thesis_degree",
    ]

    degree_types_crosswalk: ClassVar = [
        ("B.Arch", "exact", "Bachelor"),
        ("B.Arch.", "exact", "Bachelor"),
        ("B. Arch.", "exact", "Bachelor"),
        ("B.C.P", "exact", "Bachelor"),
        ("B.E.", "exact", "Bachelor"),
        (r"B.[Ss]", "regex", "Bachelor"),
        ("S.B.", "exact", "Bachelor"),
        ("D.Eng.", "exact", "Doctoral"),
        (r"Dr?.P.H.", "regex", "Doctoral"),
        ("Eng.D.", "exact", "Doctoral"),
        (r"Ph[,.\s]*?D", "regex", "Doctoral"),
        (r"S[cC]{1}[.\s]*?D", "regex", "Doctoral"),
        (r"Aero[. ]E", "regex", "Engineer"),
        (r"Bldg?.E", "regex", "Engineer"),
        (r"C(?:iv)?\.\s?E.", "regex", "Engineer"),
        ("Chem.E", "exact", "Engineer"),
        ("Chem. E.", "exact", "Engineer"),
        ("E.A.A.", "exact", "Engineer"),
        ("E.C.S.", "exact", "Engineer"),
        (r"E(?:lec)?t?.\s?E", "regex", "Engineer"),
        ("Env.E", "exact", "Engineer"),
        ("Mar.M.E.", "exact", "Engineer"),
        ("Mat.E", "exact", "Engineer"),
        ("Mech. E.", "exact", "Engineer"),
        (r"Mech.?E", "regex", "Engineer"),
        (r"Met(?:al)?.\sE", "regex", "Engineer"),
        (r"Nav.(?:A(?:rch)?|\s?E)", "regex", "Engineer"),
        (r"Nuc\.\s?E\.", "regex", "Engineer"),
        (r"Nucl?.E", "regex", "Engineer"),
        (r"Ocean[. ]{1}E", "regex", "Engineer"),
        ("San.E", "exact", "Engineer"),
        ("C.P.H.", "exact", "Master"),
        (r"M\.\s?Arch", "regex", "Master"),
        ("M.B.A.", "exact", "Master"),
        (r"M\.C\.P\.?", "regex", "Master"),
        (r"M\.\s?Eng\.", "regex", "Master"),
        (r"M\.\s?Fin\.", "regex", "Master"),
        ("M.P.H.", "exact", "Master"),
        (r"M[.\s]*?S", "regex", "Master"),
        (r"S\.\s?M\.", "regex", "Master"),
    ]

    departments_crosswalk = load_external_config(
        "dsc/workflows/digitized_theses/config/departments_crosswalk.json"
    )

    types_crosswalk: ClassVar = {"Academic thesis.": "Thesis"}

    @classmethod
    def transform(cls, source_metadata: str | bytes) -> dict:
        """Transform source metadata.

        Convert a single MARC21/XML record to Qualified Dublin Core format.

        Args:
            source_metadata: A complete MARC21/XML document of metadata from
            Alma SRU as a byte string.
        """
        record = etree.fromstring(source_metadata)

        transformed_metadata: dict[str, Any] = {}
        for field in cls.fields:
            field_method = getattr(cls, field)
            formatted_field_name = field.replace("_", ".")

            # if field method requires the record XML pass it
            if "record" in inspect.signature(field_method).parameters:
                transformed_metadata[formatted_field_name] = field_method(record)
            # else, run field method without input
            else:
                transformed_metadata[formatted_field_name] = field_method()

        return transformed_metadata

    # =========================
    #  XML navigation methods
    # =========================

    @staticmethod
    def _xpath(
        element: etree._Element,
        expr: str,
    ) -> list:
        """Run an XPath expression with the MARC namespace bound.

        This method provides the option to filtering MARC datafields by indicators.
        """
        return element.xpath(expr, namespaces=NSMAP)

    @staticmethod
    def _datafields(
        record: etree._Element,
        tag: str,
        *,
        ind1: str | int | None = None,
        ind2: str | int | None = None,
    ) -> list:
        """Return all `<marc:datafield>` elements with the given tag."""
        predicates = [f"@tag='{tag}'"]
        if ind1 is not None:
            predicates.append(f"@ind1='{ind1}'")
        if ind2 is not None:
            predicates.append(f"@ind2='{ind2}'")

        conjoined_predicates = " and ".join(predicates)

        return DigitizedThesesTransformer._xpath(
            record, f"marc:datafield[{conjoined_predicates}]"
        )

    # ==============================
    #  XML text extraction methods
    # ==============================

    @staticmethod
    def _datafield_text(datafield: etree._Element) -> str:
        """Cocatenate text for datafield across all subfields."""
        return " ".join(datafield.itertext()).strip()

    @staticmethod
    def _subfield_text(
        datafield: etree._Element,
        codes: str,
        separator: str = " ",
        *,
        first_only: bool = False,
    ) -> str:
        """Concatenate subfield values for the given codes in document order.

        Args:
            datafield: lxml Element for a `<marc:datafield>`
            codes: String of one-character subfield codes, e.g. "abcvxyz"
            separator: Joining separator (default single space)
            first_only: if True, return text of the first matching subfield only
        """
        parts: list[str] = []
        for subfield in DigitizedThesesTransformer._xpath(datafield, "marc:subfield"):
            code = subfield.get("code", "")
            if code in codes:
                text = (subfield.text or "").strip()
                if text:
                    if first_only:
                        return text
                    parts.append(text)
        return separator.join(parts).strip()

    # =================================
    #  DC element (qualified) methods
    # =================================

    @classmethod
    def dc_title(cls, record: etree._Element) -> list[str]:
        """MARC 245 and 246."""
        results: list[str] = []

        for datafield in cls._datafields(record, "245"):
            value = cls._subfield_text(datafield, "ab")
            if value:
                results.append(re.sub(r"(.*) /", r"\1", value))

        return results

    @classmethod
    def dc_date_issued(cls, record: etree._Element) -> list[str]:
        """MARC 502 (first ocurrence)."""
        results: list[str] = []

        datafields = cls._datafields(record, "502")
        if datafields:
            dissertation_note = cls._subfield_text(datafields[0], codes="a")
            degree_year = cls._subfield_text(datafields[0], codes="d")
            if dissertation_note:
                results.append(re.sub(r".*(\d{4}).*", r"\1", dissertation_note))
            if degree_year:
                results.append(degree_year)

        return results

    @classmethod
    def dc_contributor_advisor(cls, record: etree._Element) -> list[str] | None:
        """MARC 992."""
        results: list[str] = []
        for datafield in cls._datafields(record, "992", ind1=0, ind2=0):
            value = cls._subfield_text(datafield, "a")
            if value:
                results.append(re.sub(r"Supervised by (.*)", r"\1", value))

        return results

    @classmethod
    def dc_contributor_author(cls, record: etree._Element) -> list[str] | None:
        """MARC 100 and 700."""
        results: list[str] = []

        for datafield in cls._datafields(record, "100"):
            value = cls._datafield_text(datafield)
            if value:
                results.append(value)

        for datafield in cls._datafields(record, "700"):
            value = cls._subfield_text(datafield, "abd")
            if value:
                results.append(value)

        return results or None

    @classmethod
    def dc_contributor_department(cls, record: etree._Element) -> list[str] | None:
        """MARC 710 or 502 (if 710a or 710b is not set)."""
        results: list[str] = []

        for datafield in cls._datafields(record, "710"):
            value = cls._subfield_text(datafield, codes="ab")
            if value:
                results.append(re.sub(r"^(.*?)\.?(?:Thesis\.\d{4}.*)?$", r"\1", value))

        if not results:
            for datafield in cls._datafields(record, "502"):
                value = cls._subfield_text(datafield, "c")
                if value:
                    results.append(
                        value.replace(
                            "Massachusetts Institute of Technology,",
                            "Massachusetts Institute of Technology.",
                        )
                    )

        return cls._normalize_departments(results) or None

    @classmethod
    def _normalize_departments(cls, values: list[str]) -> list[str]:
        return [
            cls.departments_crosswalk.get(department, department) for department in values
        ]

    @classmethod
    def dc_contributor_other(cls, record: etree._Element) -> list[str] | None:
        """MARC 110, 111, 710, 711, and 720."""
        results: list[str] = []

        for tag in ("110", "111"):
            for datafield in cls._datafields(record, tag):
                value = cls._datafield_text(datafield)
                if value:
                    results.append(value)

        for datafield in cls._datafields(record, "710"):
            value = cls._subfield_text(datafield, codes="abcdft")
            if value:
                results.append(value)

        for datafield in cls._datafields(record, "711"):
            value = cls._subfield_text(datafield, codes="acdefpt")
            if value:
                results.append(value)

        for datafield in cls._datafields(record, "720"):
            value = cls._subfield_text(datafield, codes="a")
            if value:
                results.append(value)

        return results or None

    @classmethod
    def dc_coverage(cls, record: etree._Element) -> list[str] | None:
        """MARC 651 $a and 752 $a-$h (hierarchical place)."""
        results: list[str] = []

        for datafield in cls._datafields(record, "651"):
            value = cls._subfield_text(datafield, "a")
            if value:
                results.append(value)

        for datafield in cls._datafields(record, "752"):
            value = cls._subfield_text(datafield, "abcdefgh")
            if value:
                results.append(value)

        return results or None

    @classmethod
    def dc_coverage_spatial(cls, record: etree._Element) -> list[str] | None:
        """MARC 522 and 043."""
        results: list[str] = []

        for datafield in cls._datafields(record, "522"):
            value = cls._subfield_text(datafield, codes="a")
            if value:
                results.append(value)

        for datafield in cls._datafields(record, "043", ind1=" ", ind2=" "):
            value = cls._datafield_text(datafield)
            if value:
                results.append(value)

        return results or None

    @classmethod
    def dc_coverage_temporal(cls, record: etree._Element) -> list[str] | None:
        """MARC 513."""
        results: list[str] = []

        for datafield in cls._datafields(record, "513", ind1=" ", ind2=" "):
            value = cls._subfield_text(datafield, codes="b")
            if value:
                results.append(value)

        return results or None

    @classmethod
    def dc_date_copyright(cls, record: etree._Element) -> list[str] | None:
        """MARC 260 and 264."""
        results: list[str] = []

        for datafield in cls._datafields(record, "260", ind1=" ", ind2=" "):
            value = cls._subfield_text(datafield, codes="c")
            if value:
                results.append(re.sub(r".*(\d{4}).*", r"\1", value))

        for datafield in cls._datafields(record, "264", ind1=" ", ind2="4"):
            value = cls._subfield_text(datafield, codes="c")
            if value:
                results.append(re.sub(r"[©c](\d{4}).*", r"\1", value))

        return results or None

    @classmethod
    def dc_date_created(cls, record: etree._Element) -> list[str] | None:
        """MARC 260."""
        results: list[str] = []
        for datafield in cls._datafields(record, "260", ind1=" ", ind2=" "):
            value = cls._subfield_text(datafield, codes="g")
            if value:
                results.append(value)
        return results or None

    @classmethod
    def dc_description(cls, record: etree._Element) -> list[str] | None:
        """MARC 500, 502, 505, and 520."""
        results: list[str] = []

        # MARC 500: concatenate subfield $a text from all occurrences
        marc_500_subfield_a_combined_values = (
            " ".join(
                [
                    cls._subfield_text(datafield, codes="a")
                    for datafield in cls._datafields(record, "500")
                ]
            )
        ).strip()
        if marc_500_subfield_a_combined_values:
            results.append(marc_500_subfield_a_combined_values)

        # MARC 502: get subfield $a text, else concatenate subfield $b-g text
        for datafield in cls._datafields(record, "502"):
            if value := cls._subfield_text(datafield, codes="a"):
                results.append(value)
                continue

            value = cls._subfield_text(datafield, codes="bcdg", separator=", ")
            if value:
                results.append(f"Thesis: {value}")

        # MARC 504:
        marc_504_subfield_a_combined_values = (
            " ".join(
                [
                    cls._subfield_text(datafield, codes="a")
                    for datafield in cls._datafields(record, "504")
                ]
            )
        ).strip()
        if marc_504_subfield_a_combined_values:
            results.append(marc_504_subfield_a_combined_values)

        return results or None

    @classmethod
    def dc_description_abstract(cls, record: etree._Element) -> list[str] | None:
        """MARC 502."""
        results: list[str] = []
        for datafield in cls._datafields(record, "520", ind1="3", ind2=" "):
            value = cls._subfield_text(datafield, "ab")
            if value:
                results.append(value)

        return results or None

    @classmethod
    def dc_description_collection(cls, record: etree._Element) -> list[str] | None:
        """MARC 502."""
        results: list[str] = []

        for datafield in cls._datafields(record, "502"):
            degree_type = cls._subfield_text(datafield, "b").replace(
                "Massachusetts Institute of Technology,",
                "Massachusetts Institute of Technology.",
            )
            institution = cls._subfield_text(datafield, "c")

            value = f"{degree_type} {institution}".strip()
            if value:
                results.append(value)

        return results or None

    @classmethod
    def dc_description_degree(cls, record: etree._Element) -> list[str] | None:
        """MARC 502."""
        results: list[str] = []

        for datafield in cls._datafields(record, "502"):
            value = cls._subfield_text(datafield, codes="b")
            if value:
                results.append(value)

        return results or None

    @classmethod
    def dc_description_provenance(cls, record: etree._Element) -> list[str] | None:
        """MARC 561."""
        results: list[str] = []

        for datafield in cls._datafields(record, "561", ind1=" ", ind2=" "):
            value = cls._datafield_text(datafield)
            if value:
                results.append(value)

        return results or None

    @classmethod
    def dc_description_sponsorship(cls, record: etree._Element) -> list[str] | None:
        """MARC 536."""
        results: list[str] = []

        for datafield in cls._datafields(record, "536", ind1=" ", ind2=" "):
            value = cls._datafield_text(datafield)
            if value:
                results.append(value)

        return results or None

    @classmethod
    def dc_description_statementofresponsibility(
        cls, record: etree._Element
    ) -> list[str] | None:
        """MARC 245."""
        results: list[str] = []
        for datafield in cls._datafields(record, "245"):
            value = cls._subfield_text(datafield, "c")
            if value:
                results.append(value)

        return results or None

    @classmethod
    def dc_description_tableofcontents(cls, record: etree._Element) -> list[str] | None:
        """MARC 505."""
        results: list[str] = []

        for datafield in cls._datafields(record, "504"):
            ind1, ind2 = datafield.get("ind1", " "), datafield.get("ind2", " ")

            if (ind1 in {"0", "2", "8"} and ind2 == " ") or (
                (ind1 in {"0", "1", "2"} or ind1 == "8") and ind2 == "0"
            ):
                value = cls._subfield_text(datafield, codes="agrt")
                if value:
                    results.append(value)

        return results or None

    @classmethod
    def dc_format(cls, record: etree._Element) -> list[str] | None:
        """MARC 856."""
        results: list[str] = []

        for datafield in cls._datafields(record, "856"):
            ind1, ind2 = datafield.get("ind1", " "), datafield.get("ind2", " ")
            if ind1 == "4" and (ind2 in {" ", "0", "1", "2", "8"}):
                value = cls._subfield_text(datafield, codes="q")
                if value:
                    results.append(value)

        return results or None

    @classmethod
    def dc_format_extent(cls, record: etree._Element) -> list[str] | None:
        """MARC 300."""
        results: list[str] = []

        for datafield in cls._datafields(record, "300", ind1=" ", ind2=" "):
            value = cls._subfield_text(datafield, codes="a")
            if value:
                results.append(re.sub(r"(.*) :", r"\1", value))

        return results or None

    @classmethod
    def dc_format_medium(cls, record: etree._Element) -> list[str] | None:
        """MARC 340."""
        results: list[str] = []

        for datafield in cls._datafields(record, "340", ind1=" ", ind2=" "):
            value = cls._subfield_text(datafield, codes="a")
            if value:
                results.append(value)

        return results or None

    @classmethod
    def dc_identifier_govdoc(cls, record: etree._Element) -> list[str] | None:
        """MARC 027 and 088."""
        results: list[str] = []

        for tag in ("027", "088"):
            for datafield in cls._datafields(record, tag):
                value = cls._datafield_text(datafield)
                if value:
                    results.append(value)

        return results or None

    @classmethod
    def dc_identifier_isbn(cls, record: etree._Element) -> list[str] | None:
        """MARC 020."""
        results: list[str] = []

        for datafield in cls._datafields(record, tag="020", ind1=" ", ind2=" "):
            value = cls._subfield_text(datafield, codes="a")
            if value:
                results.append(value)

        return results or None

    @classmethod
    def dc_identifier_issn(cls, record: etree._Element) -> list[str] | None:
        """MARC 022."""
        results: list[str] = []

        for datafield in cls._datafields(record, tag="022", ind1=" ", ind2=" "):
            value = cls._subfield_text(datafield, codes="a")
            if value:
                results.append(value)

        return results or None

    @classmethod
    def dc_identifier_ismn(cls, record: etree._Element) -> list[str] | None:
        """MARC 024."""
        results: list[str] = []

        for datafield in cls._datafields(record, tag="024"):
            ind1, ind2 = datafield.get("ind1", " "), datafield.get("ind2", " ")
            if ind1 == "2" and (ind2 in {" ", "0", "1"}):
                value = cls._subfield_text(datafield, codes="a")
                if value:
                    results.append(value)

        return results or None

    @classmethod
    def dc_identifier_other(cls, record: etree._Element) -> list[str] | None:
        """MARC 024."""
        results: list[str] = []

        for datafield in cls._datafields(record, tag="024", ind1="8", ind2=" "):
            value = cls._subfield_text(datafield, codes="a")
            if value:
                results.append(value)

        return results or None

    @classmethod
    def dc_identifier_sici(cls, record: etree._Element) -> list[str] | None:
        """MARC 024."""
        results: list[str] = []

        for datafield in cls._datafields(record, tag="024"):
            ind1, ind2 = datafield.get("ind1", " "), datafield.get("ind2", " ")
            if ind1 == "4" and (ind2 in {" ", "0", "1"}):
                value = cls._subfield_text(datafield, codes="a")
                if value:
                    results.append(value)

        return results or None

    @classmethod
    def dc_identifier_uri(cls, record: etree._Element) -> list[str] | None:
        """MARC 856."""
        results: list[str] = []

        for datafield in cls._datafields(record, tag="856"):
            ind1, ind2 = datafield.get("ind1", " "), datafield.get("ind2", " ")
            if ind1 == "4" and (ind2 in {"0", "1", "2"}):
                value = cls._subfield_text(datafield, codes="u")
                if value:
                    results.append(value)

        return results or None

    @classmethod
    def dc_language(cls, record: etree._Element) -> list[str] | None:
        """MARC 041 and 546."""
        results: list[str] = []

        for datafield in cls._datafields(record, "041"):
            ind1, ind2 = datafield.get("ind1", " "), datafield.get("ind2", " ")
            if (ind1 in {"0", "1"}) and ind2 == " ":
                value = cls._subfield_text(datafield, codes="a")
                if value:
                    results.append(value)

        for datafield in cls._datafields(record, "546", ind1=" ", ind2=" "):
            value = cls._subfield_text(datafield, codes="ab")
            if value:
                results.append(value)

        return results or None

    @classmethod
    def dc_language_iso(cls, record: etree._Element) -> list[str] | None:
        """MARC 040."""
        results: list[str] = []

        for datafield in cls._datafields(record, "040"):
            value = cls._subfield_text(datafield, codes="b")
            if value:
                results.append(value)

        return results or None

    @classmethod
    def dc_publisher(cls, record: etree._Element) -> list[str]:
        """MARC 260 and 264; includes 'Massachusetts Institute of Technology'."""
        results: list[str] = ["Massachusetts Institute of Technology"]

        # 260
        for datafield in cls._datafields(record, "260", ind1=" ", ind2=" "):
            value = cls._subfield_text(datafield, "b")
            if value:
                results.append(value)

        # 264
        for datafield in cls._datafields(record, "264", ind1=" ", ind2="0"):
            value = cls._subfield_text(datafield, "b")
            if value:
                results.append(value)

        return results

    @classmethod
    def dc_relation(cls, record: etree._Element) -> list[str]:
        """MARC 580, 775, 785, and 787."""
        results: list[str] = []

        for datafield in cls._datafields(record, "580", ind1=" ", ind2=" "):
            value = cls._datafield_text(datafield)
            if value:
                results.append(value)

        for datafield in cls._datafields(record, "775", ind1="0", ind2=" "):
            value = cls._subfield_text(datafield, codes="anostx")
            if value:
                results.append(value)

        for datafield in cls._datafields(record, "785"):
            ind1, ind2 = datafield.get("ind1", " "), datafield.get("ind2", " ")
            if ind1 == "0" and (ind2 in {"1", "2", "3", "5", "7"}):
                value = cls._subfield_text(datafield, codes="gt")
                if value:
                    results.append(value)

        for datafield in cls._datafields(record, "787"):
            ind1, ind2 = datafield.get("ind1", " "), datafield.get("ind2", " ")
            if (ind1 in {"0", "1"}) and ind2 == " ":
                value = cls._subfield_text(datafield, codes="ainost")
                if value:
                    results.append(value)

        return results

    @classmethod
    def dc_relation_haspart(cls, record: etree._Element) -> list[str] | None:
        """MARC 774."""
        results: list[str] = []

        for datafield in cls._datafields(record, "774", ind1="0", ind2=" "):
            value = cls._subfield_text(datafield, codes="anostxyz")
            if value:
                results.append(value)

        return results or None

    @classmethod
    def dc_relation_isbasedon(cls, record: etree._Element) -> list[str] | None:
        """MARC 786."""
        results: list[str] = []

        for datafield in cls._datafields(record, "786", ind1="0", ind2=" "):
            value = cls._subfield_text(datafield, codes="n")
            if value:
                results.append(value)

        return results or None

    @classmethod
    def dc_relation_isformatof(cls, record: etree._Element) -> list[str] | None:
        """MARC 776."""
        results: list[str] = []

        for datafield in cls._datafields(record, "776", ind1="0", ind2=" "):
            value = cls._subfield_text(datafield, codes="anost")
            if value:
                results.append(value)

        return results or None

    @classmethod
    def dc_relation_ispartof(cls, record: etree._Element) -> list[str] | None:
        """MARC 773."""
        results: list[str] = []

        for datafield in cls._datafields(record, "773", ind1="0", ind2=" "):
            value = cls._subfield_text(datafield, codes="anostxyz")
            if value:
                results.append(value)

        return results or None

    @classmethod
    def dc_relation_ispartofseries(cls, record: etree._Element) -> list[str] | None:
        """MARC 400, 410, 411, 440, 800, 810, 811, and 830."""
        results: list[str] = []

        for tag in ("400", "410", "411", "440", "800", "810", "811", "830"):
            for datafield in cls._datafields(record, tag):
                value = cls._datafield_text(datafield)
                if value:
                    results.append(value)

        return results or None

    @classmethod
    def dc_relation_isreferencedby(cls, record: etree._Element) -> list[str] | None:
        """MARC 510."""
        results: list[str] = []

        for datafield in cls._datafields(record, "510", ind1="0", ind2=" "):
            value = cls._subfield_text(datafield, codes="a")
            if value:
                results.append(value)

        return results or None

    @classmethod
    def dc_relation_isreplacedby(cls, record: etree._Element) -> list[str] | None:
        """MARC 785."""
        results: list[str] = []

        for datafield in cls._datafields(record, "785"):
            ind1, ind2 = datafield.get("ind1", " "), datafield.get("ind2", " ")
            if ind1 == "0" and (ind2 in {"0", "4", "2", "6", "8"}):
                value = cls._subfield_text(datafield, codes="gt")
                if value:
                    results.append(value)

        return results or None

    @classmethod
    def dc_relation_replaces(cls, record: etree._Element) -> list | None:
        """MARC 780."""
        results: list[str] = []

        for datafield in cls._datafields(record, "780", ind1="0", ind2="0"):
            value = cls._subfield_text(datafield, codes="ag#nostxyz")
            if value:
                results.append(value)

        return results or None

    @classmethod
    def dc_relation_requires(cls, record: etree._Element) -> list | None:
        """MARC 538."""
        results: list[str] = []

        for datafield in cls._datafields(record, "538", ind1=" ", ind2=" "):
            value = cls._subfield_text(datafield, codes="a")
            if value:
                results.append(value)

        return results or None

    @classmethod
    def dc_relation_uri(cls, record: etree._Element) -> list[str] | None:
        """MARC 776."""
        results: list[str] = []

        for datafield in cls._datafields(record, "776", ind1="0", ind2=" "):
            value = cls._subfield_text(datafield, codes="noxyz")
            if value:
                results.append(value)

        return results or None

    @classmethod
    def dc_rights(cls, record: etree._Element) -> list[str]:
        """MARC 540; includes default statement."""
        results: list[str] = [
            "MIT theses may be protected by copyright. Please reuse MIT thesis "
            "content according to the MIT Libraries Permissions Policy, which is "
            "available through the URL provided."
        ]

        for datafield in cls._datafields(record, "540", ind1=" ", ind2=" "):
            value = cls._subfield_text(datafield, "ab")
            if value and value not in results:
                results.append(value)

        return results

    @classmethod
    def dc_rights_uri(cls) -> list[str]:
        """Defaults to MIT Theses Permissions statement."""
        return ["http://dspace.mit.edu/handle/1721.1/7582"]

    @classmethod
    def dc_source(cls, record: etree._Element) -> list[str] | None:
        """MARC 786."""
        results: list[str] = []

        for datafield in cls._datafields(record, "786"):
            value = cls._subfield_text(datafield, "at")
            if value:
                results.append(value)

        return results or None

    @classmethod
    def dc_subject(cls, record: etree._Element) -> list[str] | None:
        """MARC 653 and MARC 710 or 502 (if 710a or 710b is not set)."""
        results: list[str] = []

        # MARC 653: subject terms
        for datafield in cls._datafields(record, "653"):
            value = cls._subfield_text(datafield, codes="a")
            if value:
                results.append(value)

        # MARC 710: corporate contributor as subject
        departments = cls._get_departments_from_710(record)
        results.extend(departments)

        # MARC 502: fallback to dissertation note if no 710 fields found
        if not departments:
            results.extend(cls._get_departments_from_502(record))

        return results or None

    @classmethod
    def _get_departments_from_710(cls, record: etree._Element) -> list[str]:
        departments: list[str] = []
        mit_pattern = (
            r"\(?Massa?chusetts Instit[ui]te of Technology\.?\)?|M\.\s?I\.\s?T\."
        )
        department_pattern = r"[Dd]epartment\s[Oo]f|[Dd]ept\.\s+[Oo]f"
        thesis_pattern = r"^(.*?)(?:Thesis\.\d{4}.*)?$"

        for datafield in cls._datafields(record, "710"):
            value = cls._subfield_text(datafield, codes="ab")
            if value:
                _cleaned = re.sub(mit_pattern, "", value)
                _cleaned = re.sub(department_pattern, "", _cleaned)
                _cleaned = re.sub(thesis_pattern, r"\1", _cleaned)
                departments.append(_cleaned.strip())

        return departments

    @classmethod
    def _get_departments_from_502(cls, record: etree._Element) -> list[str]:
        departments: list[str] = []

        for datafield in cls._datafields(record, "502"):
            degree_type = cls._subfield_text(datafield, "b").replace(
                "Massachusetts Institute of Technology,",
                "Massachusetts Institute of Technology.",
            )
            institution = cls._subfield_text(datafield, "c")

            value = f"{degree_type} {institution}".strip()

            if value:
                department = re.sub(
                    r"^(?:Thesis\s\(.*\)(?:--|\s)?)?(.*?)(?:, \d{4})?\.?$",
                    r"\1",
                    value,
                )
                departments.append(
                    re.sub(r"^(?:.*Department of )?(.*)$", r"\1", department)
                )

        return departments

    @classmethod
    def dc_subject_ddc(cls, record: etree._Element) -> list[str] | None:
        """MARC 082."""
        results: list[str] = []

        for datafield in cls._datafields(record, "082"):
            value = cls._subfield_text(datafield, codes="a")
            if value:
                results.append(value)

        return results or None

    @classmethod
    def dc_subject_lcc(cls, record: etree._Element) -> list[str] | None:
        """MARC 050 and 090."""
        results: list[str] = []

        for datafield in cls._datafields(record, "050"):
            value = cls._subfield_text(datafield, codes="a")
            if value:
                results.append(value)

        for datafield in cls._datafields(record, "090", ind1=" ", ind2=" "):
            value = cls._subfield_text(datafield, codes="ab")
            if value:
                results.append(value)

        return results or None

    @classmethod
    def dc_subject_lcsh(cls, record: etree._Element) -> list[str] | None:
        """MARC 630 and 650."""
        results: list[str] = []

        for datafield in cls._datafields(record, "630"):
            value = cls._datafield_text(datafield)
            if value:
                results.append(value)

        for datafield in cls._datafields(record, "650", ind1=" ", ind2="0"):
            value = cls._datafield_text(datafield)
            if value:
                results.append(value)

        return results or None

    @classmethod
    def dc_subject_mesh(cls, record: etree._Element) -> list[str] | None:
        """Marc 650."""
        results: list[str] = []

        for datafield in cls._datafields(record, "650", ind1=" ", ind2="2"):
            value = cls._datafield_text(datafield)
            if value:
                results.append(value)

        return results or None

    @classmethod
    def dc_subject_other(cls, record: etree._Element) -> list[str] | None:
        """MARC 650."""
        results: list[str] = []

        for datafield in cls._datafields(record, "650", ind1=" ", ind2="7"):
            value = cls._subfield_text(datafield, codes="a2")
            if value:
                results.append(value)

        return results or None

    @classmethod
    def dc_title_alternative(cls, record: etree._Element) -> list[str] | None:
        """MARC 246."""
        results: list[str] = []

        for datafield in cls._datafields(record, "246"):
            value = cls._subfield_text(datafield, codes="ab")
            if value:
                results.append(value)

        return results or None

    @classmethod
    def dc_type(cls, record: etree._Element) -> list[str]:
        """MARC 655; includes 'Thesis' by default.

        This method normalizes 'Academic thesis.' to 'Thesis'
        and removes duplicates.

        NOTE: When examining sample records, it seems this field only
        extracted 'Academic thesis.' and stakeholders expressed
        preference for the term 'Thesis' instead.
        """
        results: list[str] = ["Thesis"]

        for datafield in cls._datafields(record, "655"):
            ind1, ind2 = datafield.get("ind1", " "), datafield.get("ind2", " ")
            if (ind1 == " " and (ind2 in {" ", "7"})) or (ind1 == "0" and ind2 == "7"):
                value = cls._normalize_dc_type(
                    cls._subfield_text(datafield, codes="abcvxyz")
                )
                if value and value not in results:
                    results.append(value)

        return results

    @classmethod
    def _normalize_dc_type(cls, value: str) -> str:
        return cls.types_crosswalk.get(value, value)

    @classmethod
    def mit_thesis_degree(cls, record: etree._Element) -> list[str]:
        """MARC 502, normalized using cls.degree_types_crosswalk.

        If the 502 $b value does not match any of the patterns in the
        crosswalk, the original value is returned.
        """
        results: list[str] = []

        for datafield in cls._datafields(record, "502"):
            value = cls._subfield_text(datafield, "b")
            if value:
                value = value.replace(
                    "Massachusetts Institute of Technology,",
                    "Massachusetts Institute of Technology.",
                )
                results.append(cls._normalize_degree_type(value) or value)

        return results

    @classmethod
    def _normalize_degree_type(cls, value: str) -> str | None:
        """Normalize 502 $b (degree type) value for mit.thesis.degree.

        Returns the normalized degree [Bachelor, Doctoral, Engineer, Master]
        given cls.degree_types_crosswalk. If there are no matches,
        returns None.
        """
        if not value:
            return None

        _value = value.strip()

        for pattern, method, normalized in cls.degree_types_crosswalk:
            if method == "exact" and _value == pattern:
                return normalized
            if method == "regex" and re.search(pattern, _value):
                return normalized

        return None
