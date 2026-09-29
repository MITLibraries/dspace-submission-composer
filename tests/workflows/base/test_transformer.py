import json
import re
from collections.abc import Iterable
from typing import ClassVar

import pytest

from dsc import exceptions
from dsc.workflows.base import DirectMappingTransformer, FieldMethodTransformer


class ChildDirectMappingTransformer(DirectMappingTransformer):
    fields: ClassVar[Iterable[str]] = [
        "dc.title",
        "dc.date.issued",
        "dc.contributor.author",
    ]
    delimited_fields: ClassVar[dict[str, str]] = {"dc.contributor.author": "|"}


class ChildFieldMethodTransformer(FieldMethodTransformer):
    fields: ClassVar[Iterable[str]] = ["dc.title", "dc.date.issued"]

    @classmethod
    def dc_title(cls, source_metadata: dict) -> str:
        return source_metadata["title"]

    @classmethod
    def dc_date_issued(cls, source_metadata: dict):
        return source_metadata["date"]

    @staticmethod
    def deserialize(source_metadata: str) -> dict:
        return json.loads(source_metadata)


def test_base_direct_mapping_transformer_filters_fields():
    source_metadata = {
        "dc.title": "A title",
        "dc.date.issued": "2026",
        "dc.contributor.author": "Author 1 | Author 2",
        "ignored.field": "Should not be included",
    }

    assert ChildDirectMappingTransformer.transform(source_metadata) == {
        "dc.title": "A title",
        "dc.date.issued": "2026",
        "dc.contributor.author": ["Author 1", "Author 2"],
    }


def test_base_direct_mapping_transformer_parses_delimited_fields():
    source_metadata = {
        "dc.contributor.author": "  Author One  | |Author Two|  | Author Three ",
    }

    assert ChildDirectMappingTransformer.transform(source_metadata) == {
        "dc.contributor.author": ["Author One", "Author Two", "Author Three"],
    }


def test_base_field_method_transformer_filters_fields():
    # JSON string
    source_metadata = (
        '{"title": "A title", "date": "2026", "ignored_field": "Should not be included"}'
    )

    assert ChildFieldMethodTransformer.transform(source_metadata) == {
        "dc.title": "A title",
        "dc.date.issued": "2026",
    }


def test_base_field_method_transformer_missing_field_method_raise_error():
    # JSON string
    source_metadata = (
        '{"title": "A title", "date": "2026", "ignored_field": "Should not be included"}'
    )
    ChildFieldMethodTransformer.fields.append("dc.contributor.author")

    with pytest.raises(
        exceptions.MetadataTransformationError,
        match=re.escape(
            "Error transforming field 'dc.contributor.author': No field method"
        ),
    ):
        ChildFieldMethodTransformer.transform(source_metadata)
