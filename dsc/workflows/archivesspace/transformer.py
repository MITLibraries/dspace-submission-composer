from collections.abc import Iterable

from dsc.workflows.base import Transformer


class ArchivesSpaceTransformer(Transformer):
    """Transformer for OpenCourseWare (OCW) source metadata."""

    @classmethod
    def dc_contributor_author(cls, source_metadata: dict) -> list[str]:
        return source_metadata["author"].split("|")

    @classmethod
    def optional_fields(cls) -> Iterable[str]:
        return [
            "dc.contributor.author",
            "dc.description",
            "dc.rights",
            "dc.rights.uri",
        ]
