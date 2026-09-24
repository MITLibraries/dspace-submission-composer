from dsc.workflows.base import DirectMappingTransformer


class ArchivesSpaceTransformer(DirectMappingTransformer):
    @classmethod
    def transform(cls, source_metadata: object) -> dict:
        """Transform ArchivesSpace source metadata."""
        raise NotImplementedError(
            "ArchivesSpace metadata transformation is not implemented."
        )
