from pathlib import Path
from unittest.mock import MagicMock

import pytest

from oddsfox_pipeline.config._reload_settings import reload_all_settings_modules


def test_env_int_invalid_falls_back_to_default(monkeypatch, isolated_env):
    monkeypatch.setenv("ODDS_REQUESTS_PER_SECOND", "not-an-int")
    settings = reload_all_settings_modules()
    assert settings.ODDS_REQUESTS_PER_SECOND == 40


def test_env_int_missing_uses_default(isolated_env):
    settings = reload_all_settings_modules()
    assert isinstance(settings.ODDS_REQUESTS_PER_SECOND, int)


def test_env_float_invalid_falls_back_to_default(monkeypatch, isolated_env):
    monkeypatch.setenv("HTTP_CONNECT_TIMEOUT_SECONDS", "bad-float")
    monkeypatch.setenv("HTTP_READ_TIMEOUT_SECONDS", "still-bad")
    settings = reload_all_settings_modules()
    assert settings.HTTP_CONNECT_TIMEOUT_SECONDS == 5.0
    assert settings.HTTP_READ_TIMEOUT_SECONDS == 60.0


def test_http_request_timeout_tuple_from_env(monkeypatch, isolated_env):
    monkeypatch.setenv("HTTP_CONNECT_TIMEOUT_SECONDS", "1.5")
    monkeypatch.setenv("HTTP_READ_TIMEOUT_SECONDS", "45")
    settings = reload_all_settings_modules()
    assert settings.HTTP_REQUEST_TIMEOUT == (1.5, 45.0)


def test_duckdb_path_setdefault_and_profiles_dir_default(isolated_env):
    settings = reload_all_settings_modules()
    assert settings.DUCKDB_PATH.name.endswith(".duckdb")
    assert "profiles" in str(settings.DBT_PROFILES_DIR)


def test_invalid_dbt_profiles_dir_falls_back_to_packaged_profiles(
    monkeypatch, tmp_path, isolated_env
):
    bad_profiles_dir = tmp_path / "profiles"
    bad_profiles_dir.mkdir()
    (bad_profiles_dir / "profiles.yml").write_text("other: {}\n")
    monkeypatch.setenv("DBT_PROFILES_DIR", str(bad_profiles_dir))

    settings = reload_all_settings_modules()

    assert settings.DBT_PROFILES_DIR == settings.BASE_DIR / "dbt" / "profiles"


def test_load_dotenv_not_called_when_dotenv_missing(monkeypatch, isolated_env):
    mock_load = MagicMock()
    monkeypatch.setattr("dotenv.load_dotenv", mock_load)
    real_exists = Path.exists

    def exists_stub(self):
        if self.name == ".env":
            return False
        return real_exists(self)

    monkeypatch.setattr(Path, "exists", exists_stub)

    reload_all_settings_modules()
    assert not mock_load.called


def test_load_dotenv_called_only_when_dotenv_exists(monkeypatch, isolated_env):
    """Exercise the `if env_path.exists()` branch without touching the real repo .env."""
    mock_load = MagicMock()
    monkeypatch.setattr("dotenv.load_dotenv", mock_load)
    real_exists = Path.exists

    def exists_stub(self):
        if self.name == ".env":
            return True
        return real_exists(self)

    monkeypatch.setattr(Path, "exists", exists_stub)

    reload_all_settings_modules()
    assert mock_load.called


def test_clob_env_vars_exposed(monkeypatch, isolated_env):
    monkeypatch.setenv("CLOB_API_KEY", "k")
    monkeypatch.setenv("CLOB_API_SECRET", "s")
    monkeypatch.setenv("CLOB_API_PASSPHRASE", "p")
    settings = reload_all_settings_modules()
    assert settings.CLOB_API_KEY == "k"
    assert settings.CLOB_API_SECRET == "s"
    assert settings.CLOB_API_PASSPHRASE == "p"


def test_optional_env_str_strips_and_ignores_blank(monkeypatch, isolated_env):
    settings = reload_all_settings_modules()
    monkeypatch.setenv("_ODDSFOX_OPTIONAL_ENV_STR_HELPER", "  token-value  ")
    assert settings._optional_env_str("_ODDSFOX_OPTIONAL_ENV_STR_HELPER") == (  # noqa: SLF001
        "token-value"
    )

    monkeypatch.setenv("_ODDSFOX_OPTIONAL_ENV_STR_HELPER", "   ")
    assert settings._optional_env_str("_ODDSFOX_OPTIONAL_ENV_STR_HELPER") is None  # noqa: SLF001


def test_optional_env_number_helpers_and_date_fallback(monkeypatch, isolated_env):
    settings = reload_all_settings_modules()

    monkeypatch.setenv("_ODDSFOX_OPTIONAL_ENV_FLOAT", " 1.25 ")
    assert settings._optional_env_float("_ODDSFOX_OPTIONAL_ENV_FLOAT") == 1.25  # noqa: SLF001
    monkeypatch.setenv("_ODDSFOX_OPTIONAL_ENV_FLOAT", "bad")
    assert settings._optional_env_float("_ODDSFOX_OPTIONAL_ENV_FLOAT") is None  # noqa: SLF001
    monkeypatch.setenv("_ODDSFOX_OPTIONAL_ENV_FLOAT", " ")
    assert settings._optional_env_float("_ODDSFOX_OPTIONAL_ENV_FLOAT") is None  # noqa: SLF001

    monkeypatch.setenv("_ODDSFOX_OPTIONAL_ENV_INT", " 7 ")
    assert settings._optional_env_int("_ODDSFOX_OPTIONAL_ENV_INT") == 7  # noqa: SLF001
    monkeypatch.setenv("_ODDSFOX_OPTIONAL_ENV_INT", "bad")
    assert settings._optional_env_int("_ODDSFOX_OPTIONAL_ENV_INT") is None  # noqa: SLF001
    monkeypatch.setenv("_ODDSFOX_OPTIONAL_ENV_INT", " ")
    assert settings._optional_env_int("_ODDSFOX_OPTIONAL_ENV_INT") is None  # noqa: SLF001

    monkeypatch.setenv("_ODDSFOX_ENV_DATE", "bad-date")
    assert settings._env_date("_ODDSFOX_ENV_DATE", "2026-07-19").isoformat() == (  # noqa: SLF001
        "2026-07-19"
    )


def test_env_bool_parses_truthy_and_falsey_values(monkeypatch, isolated_env):
    monkeypatch.setenv("POLYMARKET_MINUTELY_ODDS_SCHEDULE_ENABLED", "yes")
    monkeypatch.setenv("POLYMARKET_MINUTELY_ODDS_LIVE_SCHEDULE_ENABLED", "0")
    monkeypatch.setenv("POLYMARKET_HOURLY_ODDS_SCHEDULE_ENABLED", "true")
    settings = reload_all_settings_modules()
    assert settings.POLYMARKET_MINUTELY_ODDS_SCHEDULE_ENABLED is True
    assert settings.POLYMARKET_MINUTELY_ODDS_LIVE_SCHEDULE_ENABLED is False
    assert settings.POLYMARKET_HOURLY_ODDS_SCHEDULE_ENABLED is True


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("true", True),
        ("closed", True),
        ("false", False),
        ("open", False),
        ("", None),
        ("any", None),
        ("surprise", False),
    ],
)
def test_market_scope_keyset_closed_env_branches(
    monkeypatch, isolated_env, raw, expected
):
    monkeypatch.setenv("POLYMARKET_SCOPE_KEYSET_CLOSED", raw)
    settings = reload_all_settings_modules()
    assert settings.POLYMARKET_SCOPE_KEYSET_CLOSED is expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("", None), ("none", None), ("2500.5", 2500.5), ("bad", 10000.0)],
)
def test_market_scope_keyset_volume_min_env_branches(
    monkeypatch, isolated_env, raw, expected
):
    monkeypatch.setenv("POLYMARKET_SCOPE_KEYSET_VOLUME_MIN", raw)
    settings = reload_all_settings_modules()
    assert settings.POLYMARKET_SCOPE_KEYSET_VOLUME_MIN == expected


def test_market_scope_tag_crawl_denylist_empty(monkeypatch, isolated_env):
    monkeypatch.setenv("POLYMARKET_SCOPE_TAG_CRAWL_DENYLIST", " ")
    settings = reload_all_settings_modules()
    assert settings.POLYMARKET_SCOPE_TAG_CRAWL_DENYLIST == ()


def test_market_scope_tag_crawl_denylist_parses_csv(monkeypatch, isolated_env):
    monkeypatch.setenv("POLYMARKET_SCOPE_TAG_CRAWL_DENYLIST", " Sports, ,Politics ")
    settings = reload_all_settings_modules()
    assert settings.POLYMARKET_SCOPE_TAG_CRAWL_DENYLIST == ("sports", "politics")


def test_dbt_cli_argv_uses_active_interpreter():
    import sys

    from oddsfox_pipeline.config.settings_warehouse import dbt_cli_argv

    assert dbt_cli_argv("parse", "--project-dir", "dbt") == [
        sys.executable,
        "-m",
        "dbt.cli.main",
        "parse",
        "--project-dir",
        "dbt",
    ]


def test_resolve_dbt_executable_prefers_venv_script(monkeypatch, tmp_path):
    from oddsfox_pipeline.config.settings_warehouse import resolve_dbt_executable

    fake_python = tmp_path / "bin" / "python3"
    fake_python.parent.mkdir(parents=True)
    fake_python.write_text("")
    fake_dbt = fake_python.with_name("dbt")
    fake_dbt.write_text("")

    monkeypatch.setattr(
        "oddsfox_pipeline.config.settings_warehouse.sys.executable", str(fake_python)
    )
    assert resolve_dbt_executable() == str(fake_dbt)


def test_resolve_dbt_executable_falls_back_to_path(monkeypatch, tmp_path):
    from oddsfox_pipeline.config.settings_warehouse import resolve_dbt_executable

    fake_python = tmp_path / "python3"
    fake_python.write_text("")
    monkeypatch.setattr(
        "oddsfox_pipeline.config.settings_warehouse.sys.executable", str(fake_python)
    )
    monkeypatch.setattr(
        "oddsfox_pipeline.config.settings_warehouse.shutil.which",
        lambda _name: "/usr/local/bin/dbt",
    )
    assert resolve_dbt_executable() == "/usr/local/bin/dbt"


def test_resolve_dbt_executable_defaults_when_missing(monkeypatch, tmp_path):
    from oddsfox_pipeline.config.settings_warehouse import resolve_dbt_executable

    fake_python = tmp_path / "python3"
    fake_python.write_text("")
    monkeypatch.setattr(
        "oddsfox_pipeline.config.settings_warehouse.sys.executable", str(fake_python)
    )
    monkeypatch.setattr(
        "oddsfox_pipeline.config.settings_warehouse.shutil.which",
        lambda _name: None,
    )
    assert resolve_dbt_executable() == "dbt"
