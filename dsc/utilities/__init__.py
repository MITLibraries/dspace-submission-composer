from _collections_abc import dict_keys

from dsc.utilities.aws.s3 import S3Client


def build_bitstream_dict(bucket: str, file_type: str | None, prefix: str) -> dict:
    """Build a dict of potential bitstreams with an item identifier for the key.

    An underscore (if present) serves as the delimiter between the item identifier
    and any additional suffixes in the case of multiple matching bitstreams.

    Args:
        bucket: The S3 bucket containing the potential bitstreams.
        file_type: Optional parameter to filter bitstreams to specified file type.
        prefix: The S3 prefix for the potential bitstreams.
    """
    s3_client = S3Client()
    bitstreams = list(
        s3_client.files_iter(
            bucket,
            file_type=file_type if file_type else "",
            prefix=prefix,
        )
    )
    bitstream_dict: dict = {}
    for bitstream in bitstreams:
        file_name = bitstream.split("/")[-1]
        item_identifier = file_name.split("_")[0] if "_" in file_name else file_name
        bitstream_dict.setdefault(item_identifier, []).append(bitstream)
    return bitstream_dict


def match_bitstreams_to_item_identifiers(
    bitstreams: dict_keys, item_identifiers: list[str]
) -> list[str]:
    """Create list of bitstreams matched to item identifiers.

    Args:
        bitstreams: A dict of S3 files with base file IDs and full URIs.
        item_identifiers: A list of item identifiers retrieved from the batch metadata.
    """
    return [
        file_id
        for item_identifier in item_identifiers
        for file_id in bitstreams
        if file_id == item_identifier
    ]


def match_item_identifiers_to_bitstreams(
    bitstreams: dict_keys, item_identifiers: list[str]
) -> list[str]:
    """Create list of item identifers matched to bitstreams.

    Args:
        bitstreams: A dict of S3 files with base file IDs and full URIs.
        item_identifiers: A list of item identifiers retrieved from the batch metadata.
    """
    return [
        item_identifier
        for file_id in bitstreams
        for item_identifier in item_identifiers
        if file_id == item_identifier
    ]
