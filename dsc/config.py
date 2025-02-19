import logging
import os
from collections.abc import Iterable

import sentry_sdk


class Config:
    REQUIRED_ENV_VARS: Iterable[str] = [
        "WORKSPACE",
        "SENTRY_DSN",
        "AWS_REGION_NAME",
        "DSS_INPUT_QUEUE",
        "DSC_SOURCE_EMAIL",
    ]

    OPTIONAL_ENV_VARS: Iterable[str] = ["WARNING_ONLY_LOGGERS"]

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
    def dss_input_queue(self) -> str:
        value = os.getenv("DSS_INPUT_QUEUE")
        if not value:
            raise OSError("Env var 'DSS_INPUT_QUEUE' must be defined")
        return value

    @property
    def warning_only_loggers(self) -> list:
        if _excluded_loggers := os.getenv("WARNING_ONLY_LOGGERS"):
            return _excluded_loggers.split(",")
        return []

    @property
    def dsc_source_email(self) -> str:
        value = os.getenv("DSC_SOURCE_EMAIL")
        if not value:
            raise OSError("Env var 'DSC_SOURCE_EMAIL' must be defined")
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
