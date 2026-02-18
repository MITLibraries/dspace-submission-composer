from collections.abc import Iterable

from dsc.workflows.base import Transformer


class TestTransformer(Transformer):

    @classmethod
    def dc_title(cls, source_metadata) -> str:
        return source_metadata["title"]

    @classmethod
    def dc_date_issued(cls, source_metadata: dict) -> str:
        return source_metadata["date"]

    @classmethod
    def dc_contributor_author(cls, source_metadata: dict) -> list[str]:
        return source_metadata["contributor"].split("|")

    @classmethod
    def optional_fields(cls) -> Iterable[str]:
        """Dublin Core metadata fields."""
        return ["dc.contributor.author"]
