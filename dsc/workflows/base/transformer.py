import inspect
import itertools
import logging
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable
from typing import Any, final

logger = logging.getLogger(__name__)


class Transformer(ABC):
    """Base metadata transformer class.

    When defining a Transformer for a given workflow, you must:

    1. Provide an implementation of Transformer.optional_fields() class method:

       Return a list of Dublin Core elements in dot (.) notation that is
       expected in the transformed metadata. The provided values must
       use the following format: dc.element.qualifier.

    2. Provide class methods for metadata fields that (a) must be derived
       dynamically or (b) set to a static value.

       Class methods may accept 'source_metadata' as a parameter.

    3. [OPTIONAL] Provide class methods for metadata fields that are simply
       mapped from an existing column in the source metadata. If provided,
       the class method must accept 'source_metadata' as a parameter.

       Note: This is optional because the Transformer.transform() will
       attempt to apply simple 1-1 mapping if a class method is not found
       for a specified field.
    """

    # based on 'Metadata Recommendations' from DSpace docs
    required_fields: Iterable[str] = ["dc.title", "dc.date.issued"]

    @classmethod
    @abstractmethod
    def optional_fields(cls) -> Iterable[str]:
        """Dublin Core metadata fields."""

    @final
    @classmethod
    def transform(cls, source_metadata: dict) -> dict | None:
        """Transform source metadata.

        This method will iterate over all fields--combined from
        Transformer.required and Transformer.optional_fields--and either
        calls field methods (if provided) or applies simple 1-1 mapping
        to generate transformed metadata.

        If a class method is not found for a given field and it is also
        not found in 'source_metadata', the function returns None, which
        indicates that something went wrong.
        """
        transformed_metadata: dict[str, Any] = {}

        all_fields = itertools.chain(cls.required_fields, cls.optional_fields())

        for field in all_fields:
            field_method = cls.get_field_method(field)
            if field_method:
                transformed_metadata[field] = cls._call_field_method(
                    field_method, source_metadata
                )
            else:
                transformed_metadata[field] = source_metadata.get(field)

        return transformed_metadata

    @classmethod
    def get_field_method(cls, field: str) -> Callable | None:
        field_method_name = field.replace(".", "_")
        try:
            return getattr(cls, field_method_name)
        except AttributeError:
            return None

    @classmethod
    def _call_field_method(
        cls, field_method: Callable, source_metadata: dict
    ) -> Any:  # noqa: ANN401
        """Invoke field method with source metadata (as needed)."""
        # check if 'source_metadata' is in signature
        signature = inspect.signature(field_method)
        if "source_metadata" in signature.parameters:
            return field_method(source_metadata)
        return field_method()
