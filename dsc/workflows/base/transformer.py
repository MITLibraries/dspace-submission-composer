import inspect
from collections.abc import Iterable
from typing import Any, ClassVar, Protocol

from dsc.exceptions import MetadataTransformationError

type SourceMetadata = dict | str | bytes


class MetadataTransformer[T: SourceMetadata](Protocol):
    """Protocol for metadata transformer classes.

    The type parameter `T` lets subclasses narrow `source_metadata` to the
    specific subset of `SourceMetadata` they actually accept.
    """

    # metadata field names using dot notation (e.g. 'dc.title')
    fields: ClassVar[Iterable[str]] = []

    @classmethod
    def transform(cls, source_metadata: T) -> dict: ...


class DirectMappingTransformer(MetadataTransformer[dict]):
    """Transformer that relies on direct mappings to prepare metadata.

    This transformer expects source metadata that is already in the expected format
    for MIT's DSpace repos, with multi-valued fields separated by a delimiter.
    This transformer MUST be subclassed and have the `fields` and `delimited_fields`
    class variable defined.

    The `delimited_fields` variable is a dict where keys correspond to the metadata
    field name (one-to-one mapping) and the value is the delimiting character used
    to separate values.
    """

    delimited_fields: ClassVar[dict[str, str]] = {}

    @classmethod
    def transform(cls, source_metadata: dict) -> dict:
        """Transform source metadata with direct mapping.

        For fields with multiple values, values should be separated with the
        delimiter indicated for the field in `delimited_fields`.
        """
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


class FieldMethodTransformer(MetadataTransformer[dict | str | bytes]):
    """Transformer that relies on field methods to prepare metadata.

    This transformer is intended for source metadata that require more processing to
    meet the expected format for the DSpace ingest. Transformation logic is defined in
    class methods (methods decorated with `@classmethod`) that return formatted values.

    This transformer MUST be subclassed and have the `fields` class variable defined,
    along with a class method for each field.
    """

    @classmethod
    def transform(cls, source_metadata: dict | str | bytes) -> dict:
        """Transform source metadata using field methods.

        For each field, a corresponding class method is expected.
        For example: The value for the "dc.title" field is derived by a
        class method named `dc_title()`.

        NOTE: The name of the corresponding class method is always the field
        name with underscores replacing periods.
        """
        # deserialize source metadata (as needed)
        if isinstance(source_metadata, str | bytes):
            _source_metadata = cls.deserialize(source_metadata)
        else:
            _source_metadata = source_metadata

        transformed_metadata = {}
        for field in cls.fields:
            try:
                field_method = getattr(cls, field.replace(".", "_"))
            except AttributeError as exception:
                raise MetadataTransformationError(
                    f"Error transforming field '{field}': No field method"
                ) from exception

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
    def deserialize(source_metadata: str | bytes) -> Any:  # noqa: ANN401
        """Deserialize source metadata into a usable object.

        By default, the method returns the source metadata unchanged.
        Subclasses can override to deserialize from raw format (e.g., XML or JSON).
        """
        return source_metadata
