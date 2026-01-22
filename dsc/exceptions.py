class DSpaceMetadataUploadError(Exception):
    pass


class InvalidDSpaceMetadataError(Exception):
    pass


class InvalidSQSMessageError(Exception):
    pass


class InvalidWorkflowNameError(Exception):
    pass


class ItemMetadatMissingRequiredFieldError(Exception):
    pass


class ItemSubmissionCreateError(Exception):
    pass


class ItemSubmissionExistsError(Exception):
    pass


class SQSMessageSendError(Exception):
    pass


# Exceptions for 'create-batch' step
class BatchCreationFailedError(Exception):
    def __init__(self, errors: list[tuple]) -> None:
        super().__init__()
        self.errors = errors


class ItemMetadataNotFoundError(Exception):
    def __init__(self) -> None:
        super().__init__("No metadata found for the item submission")


class ItemBitstreamsNotFoundError(Exception):
    def __init__(self) -> None:
        super().__init__("No bitstreams found for the item submission")
