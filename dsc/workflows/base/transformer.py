from collections.abc import Iterable
from typing import Any, ClassVar, Protocol


class MetadataTransformer(Protocol):
    """Protocol for metadata transformer classes."""

    fields: ClassVar[Iterable[str]] = []

    @classmethod
    def transform(cls, source_metadata: Any) -> dict:  # noqa: ANN401
        ...
