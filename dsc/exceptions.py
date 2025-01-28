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
