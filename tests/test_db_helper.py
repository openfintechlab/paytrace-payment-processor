import importlib

import pytest

from src.utilities.ConfigLoader import ConfigLoader
from src.utilities.DBHelper import DBHelper


db_helper_module = importlib.import_module("src.utilities.DBHelper")


@pytest.fixture(autouse=True)
def reset_db_helper_state(monkeypatch):
    monkeypatch.setattr(ConfigLoader._env, "read_env", lambda *args, **kwargs: None)
    ConfigLoader.configurations = {}
    DBHelper.dispose_connection()
    yield
    ConfigLoader.configurations = {}
    DBHelper.dispose_connection()


def test_build_connection_url_and_schema_uses_default_schema(monkeypatch):
    monkeypatch.setenv("OFTL_POSTGRESDB_USERNAME", "user")
    monkeypatch.setenv("OFTL_POSTGRESDB_PASSWORD", "pass")
    monkeypatch.setenv("OFTL_POSTGRESDB_HOST", "localhost")
    monkeypatch.setenv("OFTL_POSTGRESDB_PORT", "5432")
    monkeypatch.setenv("OFTL_POSTGRESDB_NAME", "paytrace")
    monkeypatch.delenv("OFTL_POSTGRESDB_SCHEMA", raising=False)
    ConfigLoader.configurations = {}

    connection_url, schema = DBHelper._build_connection_url_and_schema()

    assert connection_url == "postgresql+psycopg2://user:pass@localhost:5432/paytrace"
    assert schema == "default"


def test_build_connection_url_and_schema_uses_configured_schema(monkeypatch):
    monkeypatch.setenv("OFTL_POSTGRESDB_USERNAME", "user")
    monkeypatch.setenv("OFTL_POSTGRESDB_PASSWORD", "pass")
    monkeypatch.setenv("OFTL_POSTGRESDB_HOST", "localhost")
    monkeypatch.setenv("OFTL_POSTGRESDB_PORT", "5432")
    monkeypatch.setenv("OFTL_POSTGRESDB_NAME", "paytrace")
    monkeypatch.setenv("OFTL_POSTGRESDB_SCHEMA", "payments")
    ConfigLoader.configurations = {}

    connection_url, schema = DBHelper._build_connection_url_and_schema()

    assert connection_url == "postgresql+psycopg2://user:pass@localhost:5432/paytrace"
    assert schema == "payments"


def test_initialize_connection_passes_search_path(monkeypatch):
    monkeypatch.setenv("OFTL_POSTGRESDB_USERNAME", "user")
    monkeypatch.setenv("OFTL_POSTGRESDB_PASSWORD", "pass")
    monkeypatch.setenv("OFTL_POSTGRESDB_HOST", "localhost")
    monkeypatch.setenv("OFTL_POSTGRESDB_PORT", "5432")
    monkeypatch.setenv("OFTL_POSTGRESDB_NAME", "paytrace")
    monkeypatch.setenv("OFTL_POSTGRESDB_SCHEMA", "payments")
    ConfigLoader.configurations = {}

    captured: dict[str, object] = {}

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, statement):
            captured["statement"] = str(statement)
            return 1

    class FakeEngine:
        def connect(self):
            return FakeConnection()

        def dispose(self):
            captured["disposed"] = True

    monkeypatch.setattr(
        db_helper_module,
        "create_engine",
        lambda url, **kwargs: captured.update({"url": url, "kwargs": kwargs}) or FakeEngine(),
    )

    assert DBHelper.initialize_connection() is True
    assert captured["url"] == "postgresql+psycopg2://user:pass@localhost:5432/paytrace"
    assert captured["kwargs"]["connect_args"] == {"options": "-csearch_path=payments"}
