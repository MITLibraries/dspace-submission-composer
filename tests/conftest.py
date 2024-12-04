from io import StringIO

import pytest
from click.testing import CliRunner

from dsc.config import Config


@pytest.fixture(autouse=True)
def _test_env(monkeypatch):
    monkeypatch.setenv("SENTRY_DSN", "None")
    monkeypatch.setenv("WORKSPACE", "test")
    monkeypatch.setenv("AWS_REGION_NAME", "us-east-1")


@pytest.fixture
def config_instance() -> Config:
    return Config()


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def stream():
    return StringIO()
