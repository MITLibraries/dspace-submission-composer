import logging

import pytest


def test_check_required_env_vars(monkeypatch, config_instance):
    monkeypatch.delenv("WORKSPACE")
    with pytest.raises(OSError, match="Missing required environment variables:"):
        config_instance.check_required_env_vars()


def test_configure_logger_not_verbose(config_instance, stream):
    logger = logging.getLogger(__name__)
    result = config_instance.configure_logger(logger, stream, verbose=False)
    assert logger.getEffectiveLevel() == logging.INFO
    assert result == "Logger 'tests.test_config' configured with level=INFO"
    stream.seek(0)
    assert next(stream) == "INFO\n"


def test_configure_logger_verbose(config_instance, stream):
    logger = logging.getLogger(__name__)
    result = config_instance.configure_logger(logger, stream, verbose=True)
    assert logger.getEffectiveLevel() == logging.DEBUG
    assert result == "Logger 'tests.test_config' configured with level=DEBUG"
    stream.seek(0)
    assert next(stream) == "DEBUG\n"


def test_configure_sentry_no_env_variable(monkeypatch, config_instance):
    monkeypatch.delenv("SENTRY_DSN", raising=False)
    result = config_instance.configure_sentry()
    assert result == "No Sentry DSN found, exceptions will not be sent to Sentry"


def test_configure_sentry_env_variable_is_none(monkeypatch, config_instance):
    monkeypatch.setenv("SENTRY_DSN", "None")
    result = config_instance.configure_sentry()
    assert result == "No Sentry DSN found, exceptions will not be sent to Sentry"


def test_configure_sentry_env_variable_is_dsn(monkeypatch, config_instance):
    monkeypatch.setenv("SENTRY_DSN", "https://1234567890@00000.ingest.sentry.io/123456")
    result = config_instance.configure_sentry()
    assert result == "Sentry DSN found, exceptions will be sent to Sentry with env=test"
