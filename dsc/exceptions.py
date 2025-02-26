class InvalidDSpaceMetadataError(Exception):
    pass


class InvalidSQSMessageError(Exception):
    pass


class InvalidWorkflowNameError(Exception):
    pass


class ItemMetadatMissingRequiredFieldError(Exception):
    pass


class ReconcileError(Exception):
    pass


class ReconcileFoundBitstreamsWithoutMetadataWarning(Warning):
    def __init__(self, bitstreams_without_metadata: list[str]):
        self.bitstreams_without_metadata = bitstreams_without_metadata
        super().__init__(str(self))

    def __str__(self) -> str:
        """Display message listing (<=20) bitstreams without metadata."""
        message = "Bitstreams without metadata"
        if len(self.bitstreams_without_metadata) > 20:  # noqa: PLR2004
            message = f"{message} (showing first 20 bitstreams)"
        return f"{message}: {self.bitstreams_without_metadata}"


class ReconcileFoundMetadataWithoutBitstreamsWarning(Warning):

    def __init__(self, metadata_without_bitstreams: list[str]):
        self.metadata_without_bitstreams = metadata_without_bitstreams
        super().__init__(str(self))

    def __str__(self) -> str:
        """Display message listing (<=20) metadata without bitstreams."""
        message = "Metadata without bitstreams"
        if len(self.metadata_without_bitstreams) > 20:  # noqa: PLR2004
            message = f"{message} (showing first 20 metadata item identifiers)"
        return f"{message}: {self.metadata_without_bitstreams}"
