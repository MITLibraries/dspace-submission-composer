import json
import logging
import os
from collections.abc import Iterable

import sentry_sdk

METRICS_NAMESPACE = "dso"

METRICS = [
    "item_submitted",  # item submitted to DSS
    "submission_error",  # error during submission to DSS
    "ingested_item",  # item ingested successfully into DSpace
    "ingest_error",  # error during attempted item ingest into DSpace
]


class Config:
    REQUIRED_ENV_VARS: Iterable[str] = [
        "WORKSPACE",
        "SENTRY_DSN",
        "AWS_REGION_NAME",
        "ITEM_SUBMISSIONS_TABLE_NAME",
        "S3_BUCKET_SUBMISSION_ASSETS",
        "SOURCE_EMAIL",
        "SQS_QUEUE_DSS_INPUT",
        # workflow-specific
        "DSPACE_CREDENTIALS",
        "METADATA_API_URL",
        "S3_BUCKET_DIGITIZED_THESES",
    ]

    OPTIONAL_ENV_VARS: Iterable[str] = [
        "RETRY_THRESHOLD",
        "S3_BUCKET_SYNC_SOURCE",
        "WARNING_ONLY_LOGGERS",
    ]

    @property
    def workspace(self) -> str:
        return os.getenv("WORKSPACE", "dev")

    @property
    def sentry_dsn(self) -> str:
        return os.getenv("SENTRY_DSN", "None")

    @property
    def aws_region_name(self) -> str:
        return os.getenv("AWS_REGION_NAME", "us-east-1")

    @property
    def item_submissions_table_name(self) -> str:
        value = os.getenv("ITEM_SUBMISSIONS_TABLE_NAME")
        if not value:
            raise OSError("Env var 'ITEM_SUBMISSIONS_TABLE_NAME' must be defined")
        return value

    @property
    def retry_threshold(self) -> int:
        return int(os.getenv("RETRY_THRESHOLD", "20"))

    @property
    def s3_bucket_submission_assets(self) -> str:
        value = os.getenv("S3_BUCKET_SUBMISSION_ASSETS")
        if not value:
            raise OSError("Env var 'S3_BUCKET_SUBMISSION_ASSETS' must be defined")
        return value

    @property
    def s3_bucket_sync_source(self) -> str | None:
        return os.getenv("S3_BUCKET_SYNC_SOURCE")

    @property
    def source_email(self) -> str:
        value = os.getenv("SOURCE_EMAIL")
        if not value:
            raise OSError("Env var 'SOURCE_EMAIL' must be defined")
        return value

    @property
    def sqs_queue_dss_input(self) -> str:
        value = os.getenv("SQS_QUEUE_DSS_INPUT")
        if not value:
            raise OSError("Env var 'SQS_QUEUE_DSS_INPUT' must be defined")
        return value

    @property
    def warning_only_loggers(self) -> list:
        if _excluded_loggers := os.getenv("WARNING_ONLY_LOGGERS"):
            return _excluded_loggers.split(",")
        return []

    # Workflow-specific env vars
    @property
    def dspace_credentials(self) -> dict:
        value = os.getenv("DSPACE_CREDENTIALS")
        if not value:
            raise OSError("Env var 'DSPACE_CREDENTIALS' must be defined")
        credentials = json.loads(value)

        return {"IR-8": credentials["ir-8"], "DDC-8": credentials["ddc-8"]}

    @property
    def metadata_api_url(self) -> str:
        value = os.getenv("METADATA_API_URL")
        if not value:
            raise OSError("Env var 'METADATA_API_URL' must be defined")
        return value

    @property
    def s3_bucket_digitized_theses(self) -> str:
        value = os.getenv("S3_BUCKET_DIGITIZED_THESES")
        if not value:
            raise OSError("Env var 'S3_BUCKET_DIGITIZED_THESES' must be defined")
        return value

    def check_required_env_vars(self) -> None:
        """Method to raise exception if required env vars not set."""
        missing_vars = [var for var in self.REQUIRED_ENV_VARS if not os.getenv(var)]
        if missing_vars:
            message = f"Missing required environment variables: {', '.join(missing_vars)}"
            raise OSError(message)

    def configure_logger(
        self,
        root_logger: logging.Logger,
        *,
        verbose: bool = False,
    ) -> str:
        """Configure application via passed application root logger.

        If verbose=True, third-party libraries can be quite chatty. For convenience, the
        loggers for specified libraries can be set to WARNING level by assigning a
        comma-separated list of logger names to the env var WARNING_ONLY_LOGGERS.
        """
        if verbose:
            root_logger.setLevel(logging.DEBUG)
            log_format = (
                "%(asctime)s %(levelname)s %(name)s.%(funcName)s() "
                "line %(lineno)d: %(message)s"
            )
        else:
            root_logger.setLevel(logging.INFO)
            log_format = "%(asctime)s %(levelname)s %(name)s.%(funcName)s(): %(message)s"

        if self.warning_only_loggers:
            for name in self.warning_only_loggers:
                logging.getLogger(name).setLevel(logging.WARNING)

        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(log_format))
        root_logger.addHandler(handler)

        return (
            f"Logger '{root_logger.name}' configured with level="
            f"{logging.getLevelName(root_logger.getEffectiveLevel())}"
        )

    def configure_sentry(self) -> str:
        env = self.workspace
        sentry_dsn = self.sentry_dsn
        if sentry_dsn and sentry_dsn.lower() != "none":
            sentry_sdk.init(sentry_dsn, environment=env)
            return f"Sentry DSN found, exceptions will be sent to Sentry with env={env}"
        return "No Sentry DSN found, exceptions will not be sent to Sentry"


def load_external_config(file_path: str) -> dict:
    """Load a JSON configuration file into dict."""
    with open(file_path, "rb") as config_file:
        return json.load(config_file)
