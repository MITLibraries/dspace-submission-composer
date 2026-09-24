import inspect
from collections.abc import Iterable
from typing import Any, ClassVar, Protocol

from dsc.exceptions import MetadataTransformationError


class MetadataTransformer(Protocol):
    """Protocol for metadata transformer classes."""

    # metadata field names using dot notation (e.g. 'dc.title')
    fields: ClassVar[Iterable[str]] = []

    @classmethod
    def transform(cls, source_metadata: Any) -> dict:  # noqa: ANN401
        ...


class BaseTransformer(MetadataTransformer):
    # metadata field names using dot notation (e.g. 'dc.title')
    fields: ClassVar[Iterable[str]] = []

    @classmethod
    def transform(cls, source_metadata: Any) -> dict:  # noqa: ANN401
        """Transform source metadata using field methods.

        For each field, a corresponding class method is expected.
        For example: The value for the "dc.title" field is derived by a
        class method named `dc_title()`.

        NOTE: The name of the corresponding class method is always the field
        name with underscores replacing periods.
        """
        # deserialize source metadata (as needed)
        _source_metadata = cls.deserialize(source_metadata)

        transformed_metadata = {}
        for field in cls.fields:
            field_method = getattr(cls, field.replace(".", "_"))
            try:
                # if field method requires the source metadata, pass it
                if "source_metadata" in inspect.signature(field_method).parameters:
                    transformed_metadata[field] = field_method(_source_metadata)
                else:
                    # else run field method without input
                    transformed_metadata[field] = field_method()
            except Exception as exception:
                raise MetadataTransformationError(
                    f"Error transforming field '{field}': {exception}"
                ) from exception

        return transformed_metadata

    @staticmethod
    def deserialize(source_metadata: Any) -> Any:  # noqa: ANN401
        """Deserialize source metadata into a usable object.

        By default, the method returns the source metadata unchanged.
        Subclasses can override to deserialize from raw format (e.g., XML or JSON).
        """
        return source_metadata
