from dsc.workflows.simple_csv.transformer import SimpleCSVTransformer


class ArchivesSpaceTransformer(SimpleCSVTransformer):
    @classmethod
    def transform(cls, source_metadata: object) -> dict:
        """Transform ArchivesSpace source metadata."""
        raise NotImplementedError(
            "ArchivesSpace metadata transformation is not implemented."
        )
