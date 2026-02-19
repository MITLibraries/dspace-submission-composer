from collections.abc import Iterable

from dsc.workflows.base import Transformer


class SCCSTransformer(Transformer):
    """Transformer for SCCS source metadata."""

    @classmethod
    def optional_fields(cls) -> Iterable[str]:
        return [
            "dc.publisher",
            "dc.identifier.mitlicense",
            "dc.eprint.version",
            "dc.type",
            "dc.type.uri",
            "dc.source",
            "dc.contributor.author",
            "dc.contributor.department",
            "dc.relation.isversionof",
            "dc.relation.journal",
            "dc.identifier.issn",
            "dc.date.submitted",
            "dc.rights",
            "dc.rights.uri",
            "dc.description",
            "dc.description.sponsorship",
        ]

    @classmethod
    def dc_contributor_author(cls, source_metadata: dict) -> list[str]:
        return source_metadata["dc.contributor.author"].split("|")
