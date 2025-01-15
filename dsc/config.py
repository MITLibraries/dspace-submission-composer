import logging
import os
from collections.abc import Iterable
from io import StringIO

import sentry_sdk


class Config:
    REQUIRED_ENV_VARS: Iterable[str] = [
        "WORKSPACE",
        "SENTRY_DSN",
        "AWS_REGION_NAME",
        "DSS_INPUT_QUEUE",
        "DSC_SOURCE_EMAIL",
    ]

    OPTIONAL_ENV_VARS: Iterable[str] = ["LOG_LEVEL"]

    @property
    def workspace(self) -> str:
        return os.getenv("WORKSPACE", "us-east-1")

    @property
    def sentry_dsn(self) -> str:
        return os.getenv("SENTRY_DSN", "None")

    @property
    def aws_region_name(self) -> str:
        return os.getenv("AWS_REGION_NAME", "us-east-1")

    @property
    def dss_input_queue(self) -> str:
        return os.getenv("DSS_INPUT_QUEUE", "")

    @property
    def dsc_source_email(self) -> str:
        return os.getenv("DSC_SOURCE_EMAIL", "")

    def check_required_env_vars(self) -> None:
        """Method to raise exception if required env vars not set."""
        missing_vars = [var for var in self.REQUIRED_ENV_VARS if not os.getenv(var)]
        if missing_vars:
            message = f"Missing required environment variables: {', '.join(missing_vars)}"
            raise OSError(message)

    def configure_logger(
        self, logger: logging.Logger, stream: StringIO, *, verbose: bool
    ) -> str:
        logging_format_base = "%(asctime)s %(levelname)s %(name)s.%(funcName)s()"
        if verbose:
            log_method, log_level = logger.debug, logging.DEBUG
            template = logging_format_base + " line %(lineno)d: %(message)s"
            for handler in logging.root.handlers:
                handler.addFilter(logging.Filter("dsc"))
        else:
            log_method, log_level = logger.info, logging.INFO
            template = logging_format_base + ": %(message)s"

        logger.setLevel(log_level)
        logging.basicConfig(format=template)
        logger.addHandler(logging.StreamHandler(stream))
        log_method(f"{logging.getLevelName(logger.getEffectiveLevel())}")

        return (
            f"Logger '{logger.name}' configured with level="
            f"{logging.getLevelName(logger.getEffectiveLevel())}"
        )

    def configure_sentry(self) -> str:
        env = self.workspace
        sentry_dsn = self.sentry_dsn
        if sentry_dsn and sentry_dsn.lower() != "none":
            sentry_sdk.init(sentry_dsn, environment=env)
            return f"Sentry DSN found, exceptions will be sent to Sentry with env={env}"
        return "No Sentry DSN found, exceptions will not be sent to Sentry"
