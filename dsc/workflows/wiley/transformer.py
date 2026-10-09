import json
from collections.abc import Iterable
from datetime import UTC, datetime
from itertools import chain
from typing import ClassVar

from dsc.workflows.base.transformer import FieldMethodTransformer


class WileyTransformer(FieldMethodTransformer):
    """Transformer for Wiley source metadata.

    The transformer expects a dict as its input and returns a dict
    containing the metadata fields registered in DSpace. Metadata
    is sourced from the Crossref REST API:
    https://www.crossref.org/documentation/retrieve-metadata/rest-api/
    """

    fields: ClassVar[Iterable[str]] = [
        # fields with derived values
        "dc.title",
        "dc.date.issued",
        "dc.contributor.author",
        "dc.title.alternative",
        # fields with fixed values
        "dc.publisher",
        "dc.identifier.issn",
        "dc.relation.journal",
        "mit.journal.volume",
        "mit.journal.issue",
        "dc.language",
        "dc.relation.isversionof",
    ]

    @staticmethod
    def deserialize(source_metadata: str | bytes) -> dict:
        return json.loads(source_metadata)

    # ========================
    # Metadata field methods
    # ========================

    @classmethod
    def dc_title(cls, source_metadata: dict) -> str:
        """Build a title string from title components."""
        return ". ".join(
            title_components for title_components in source_metadata["title"]
        )

    @classmethod
    def dc_date_issued(cls, source_metadata: dict) -> str:
        """Return a date string using date components from 'issued' field.

        If day is not provided in the metadata, a date string formatted as
        "%Y-%m" (no day) is returned.

        Example:
            Input: {"issued": {'date-parts': [[2019, 2, 8]]}}
            Output: "2019-02-08"
        """
        date_components = dict(
            zip(
                ("year", "month", "day"),
                source_metadata["issued"]["date-parts"][0],
                strict=False,
            )
        )
        if date_components.get("day"):
            issued = datetime(
                date_components["year"],
                date_components["month"],
                date_components["day"],
                tzinfo=UTC,
            )
            return issued.strftime("%Y-%m-%d")

        issued = datetime(
            date_components["year"], date_components["month"], 1, tzinfo=UTC
        )  # day is required, use 1
        return issued.strftime("%Y-%m")

    @classmethod
    def dc_contributor_author(cls, source_metadata: dict) -> list[str] | None:
        """Return a list of formatted instructor names.

        Example:
            Input: {"author": [{"given": "Marsha", "family": "Mellow", ...}]}
            Output: "Mellow, Marsha"
        """
        return [
            author_name
            for author in source_metadata["author"]
            if (author_name := cls._format_author_name(author))
        ] or None

    @classmethod
    def _format_author_name(cls, name_components: dict[str, str]) -> str:
        """Format author name as 'family, given'.

        Example:
            Input: {"given": "Marsha", "family": "Mellow}
            Output: "Mellow, Marsha"
        """
        if not (family := name_components.get("family")) or not (
            given := name_components.get("given")
        ):
            return ""
        author_name = f"{family}, {given}"
        return author_name.strip()

    @classmethod
    def dc_title_alternative(cls, source_metadata: dict) -> list[str] | None:
        """Return a list of alternative titles from multiple list fields."""
        alternative_title_lists = [
            source_metadata.get("original-title"),
            source_metadata.get("short-title"),
            source_metadata.get("subtitle"),
        ]

        return (
            list(
                chain.from_iterable(
                    alternative_titles
                    for alternative_titles in alternative_title_lists
                    if alternative_titles
                )
            )
            or None
        )

    @classmethod
    def dc_publisher(cls, source_metadata: dict) -> str | None:
        return source_metadata.get("publisher")

    @classmethod
    def dc_identifier_issn(cls, source_metadata: dict) -> list[str] | None:
        return source_metadata.get("ISSN")

    @classmethod
    def dc_relation_journal(cls, source_metadata: dict) -> list[str] | None:
        return source_metadata.get("container-title")

    @classmethod
    def mit_journal_volume(cls, source_metadata: dict) -> str | None:
        return source_metadata.get("volume")

    @classmethod
    def mit_journal_issue(cls, source_metadata: dict) -> str | None:
        return source_metadata.get("issue")

    @classmethod
    def dc_language(cls, source_metadata: dict) -> str | None:
        return source_metadata.get("language")

    @classmethod
    def dc_relation_isversionof(cls, source_metadata: dict) -> str | None:
        return source_metadata.get("URL")
