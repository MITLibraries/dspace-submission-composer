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
    ]

    OPTIONAL_ENV_VARS: Iterable[str] = ["LOG_LEVEL"]

    def __getattr__(self, name: str) -> str | None:
        """Provide dot notation access to configurations and env vars on this class."""
        if name in self.REQUIRED_ENV_VARS or name in self.OPTIONAL_ENV_VARS:
            return os.getenv(name)
        message = f"'{name}' not a valid configuration variable"
        raise AttributeError(message)

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
        logger.addHandler(logging.StreamHandler(stream))

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
        env = self.WORKSPACE
        sentry_dsn = self.SENTRY_DSN
        if sentry_dsn and sentry_dsn.lower() != "none":
            sentry_sdk.init(sentry_dsn, environment=env)
            return f"Sentry DSN found, exceptions will be sent to Sentry with env={env}"
        return "No Sentry DSN found, exceptions will not be sent to Sentry"
