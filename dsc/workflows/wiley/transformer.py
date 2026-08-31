import inspect
import json
from collections.abc import Iterable
from datetime import UTC, datetime
from itertools import chain

from dsc.exceptions import MetadataTransformationError


class WileyTransformer:
    fields: Iterable[str] = [
        # fields with derived values
        "dc_title",
        "dc_date_issued",
        "dc_contributor_author",
        "dc_title_alternative",
        # fields with fixed values
        "dc_publisher",
        "dc_identifier_issn",
        "dc_relation_journal",
        "mit_journal_volume",
        "mit_journal_issue",
        "dc_language",
        "dc_relation_isversionof",
    ]

    @classmethod
    def transform(cls, source_metadata: str | bytes) -> dict:
        """Transform source metadata."""
        record = json.loads(source_metadata)
        transformed_metadata: dict[str, list] = {}

        for field in cls.fields:
            field_method = getattr(cls, field)
            formatted_field_name = field.replace("_", ".")
            try:
                # if field method requires the record XML pass it
                if "record" in inspect.signature(field_method).parameters:
                    transformed_metadata[formatted_field_name] = field_method(record)
                # else, run field method without input
                else:
                    transformed_metadata[formatted_field_name] = field_method()
            except Exception as exception:
                raise MetadataTransformationError(
                    f"Error transforming field '{formatted_field_name}': {exception}"
                ) from exception

        return transformed_metadata

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
            date = datetime.date(*date_components.values())
            return date.strftime("%Y-%m-%d")

        date = datetime(
            date_components["year"], date_components["month"], 1, tzinfo=UTC
        )  # day is required, use 1
        return date.strftime("%Y-%m")

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
        return source_metadata.get("dc_relation_isversionof")
