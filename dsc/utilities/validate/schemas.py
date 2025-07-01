RESULT_MESSAGE_ATTRIBUTES = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "DSS Result Message - MessageAttributes",
    "description": "Content of result message 'MessageAttributes' from DSS",
    "type": "object",
    "properties": {
        "PackageID": {
            "type": "object",
            "properties": {
                "DataType": {"type": "string", "const": "String"},
                "StringValue": {"type": "string"},
            },
            "required": ["DataType", "StringValue"],
        },
        "SubmissionSource": {
            "type": "object",
            "properties": {
                "DataType": {"type": "string", "const": "String"},
                "StringValue": {"type": "string"},
            },
            "required": ["DataType", "StringValue"],
        },
    },
    "required": ["PackageID", "SubmissionSource"],
}

RESULT_MESSAGE_BODY = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "DSS Result Message - Body",
    "description": "Content of result message 'Body' from DSS",
    "type": "object",
    "properties": {
        "ResultType": {"type": "string"},
        "ItemHandle": {"type": ["string", "null"]},
        "lastModified": {"type": "string"},
        "Bitstreams": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "BitstreamName": {"type": "string"},
                    "BitstreamUUID": {"type": "string"},
                    "BitstreamChecksum": {
                        "type": "object",
                        "properties": {
                            "value": {"type": "string"},
                            "checkSumAlgorithm": {"type": "string"},
                        },
                    },
                },
            },
        },
        "ErrorTimestamp": {"type": "string"},
        "ErrorInfo": {"type": "string"},
        "DSpaceResponse": {"type": "string"},
        "ExceptionTraceback": {"type": "array", "items": {"type": "string"}},
    },
    "if": {"properties": {"ResultType": {"const": "success"}}},
    "then": {"required": ["lastModified", "Bitstreams"]},
    "else": {
        "required": [
            "ErrorTimestamp",
            "ErrorInfo",
            "DSpaceResponse",
            "ExceptionTraceback",
        ]
    },
}
