from dsc.workflows.base.transformer import MetadataTransformer


class WileyTransformer(MetadataTransformer):
    @classmethod
    def transform(cls, source_metadata: object) -> dict:
        """Transform Wiley source metadata."""
        raise NotImplementedError("Wiley metadata transformation is not implemented.")
