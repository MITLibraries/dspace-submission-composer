from collections.abc import Iterable
from typing import Any, ClassVar, Protocol


class MetadataTransformer(Protocol):
    """Protocol for metadata transformer classes."""

    fields: ClassVar[Iterable[str]] = []

    @classmethod
    def transform(cls, source_metadata: Any) -> dict:  # noqa: ANN401
        ...


class BaseTransformer(MetadataTransformer):
    """Base transformer for workflows where field names correspond to DSpace field names.

    Subclasses declare which fields to include and which are pipe-delimited.
    """

    delimited_fields: ClassVar[dict[str, str]] = {}

    @classmethod
    def transform(cls, source_metadata: dict) -> dict:
        """Transform source metadata."""
        transformed_metadata = {}
        for field in cls.fields:
            value = source_metadata.get(field)
            if value is None:
                continue
            if field in cls.delimited_fields:
                delimiter = cls.delimited_fields[field]
                transformed_metadata[field] = [
                    v.strip() for v in value.split(delimiter) if v.strip()
                ]
            else:
                transformed_metadata[field] = value
        return transformed_metadata
