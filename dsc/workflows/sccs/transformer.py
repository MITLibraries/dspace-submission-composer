from typing import ClassVar

from dsc.workflows.base import BaseTransformer


class SCCSTransformer(BaseTransformer):
    """Transformer for SCCS source metadata."""

    fields: ClassVar[list[str]] = [
        "dc.title",
        "dc.publisher",
        "dc.identifier.mitlicense",
        "dc.eprint.version",
        "dc.type",
        "dc.type.uri",
        "dc.source",
        "dc.contributor.author",
        "dc.relation.orgunit",
        "dc.relation.isversionof",
        "dc.relation.journal",
        "dc.identifier.issn",
        "dc.date.issued",
        "dc.date.submitted",
        "dc.rights",
        "dc.rights.uri",
        "dc.description",
        "dc.description.sponsorship",
    ]

    delimited_fields: ClassVar[dict[str, str]] = {
        "dc.contributor.author": "|",
    }
