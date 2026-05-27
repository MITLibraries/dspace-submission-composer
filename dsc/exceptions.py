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


# Exceptions for DSpace client
class DSpaceClientError(Exception):
    """General exception raised when DSpace client action results in error."""


class DSpaceClientCredentialsNotFoundError(DSpaceClientError):
    """Raise when env var DSPACE_CREDENTIALS does not include submission system."""


class DSpaceClientAuthenticationError(DSpaceClientError):
    """Raise when DSpace client fails authentication.

    Authentication may fail due to 401 Unauthorized or 403 Forbidden errors.
    """

    def __init__(
        self,
        dspace_url: str | float | None,
        dspace_user: str | float | None,
    ):
        self.message = (
            f"Failed to authenticate to DSpace server at '{dspace_url}' with user "
            f"'{dspace_user}'. Please verify that the DSPACE_CREDENTIALS "
            "environment variable is set correctly and that the DSpace server is "
            "accessible."
        )


class DSpaceClientSearchError(DSpaceClientError):
    """Raise when DSpace client search operation results in error.

    Search is performed by dspace_rest_client.client.search_objects,
    which returns None if the response from a GET request returns an
    exit code other than 200.
    """


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
