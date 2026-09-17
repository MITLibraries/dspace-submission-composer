from typing import ClassVar

from dsc.workflows.base.transformer import MetadataTransformer


class SimpleCSVTransformer(MetadataTransformer):
    """Simple CSV transformer for workflows where fields align with DSpace field names.

    This transformer MUST be subclassed and have the `fields` class variable defined.

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
