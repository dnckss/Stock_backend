"""config.py + validate_required_env 단위 테스트."""
import importlib
import os

import pytest


def _reload_config(monkeypatch):
    """config 를 reload — .env 파일이 monkeypatch 한 env 를 덮어쓰지 않게 load_dotenv mock."""
    import dotenv
    monkeypatch.setattr(dotenv, "load_dotenv", lambda *a, **k: False)
    import config
    return importlib.reload(config)


def test_validate_required_env_passes_when_set(monkeypatch):
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_KEY", "test")
    monkeypatch.setenv("STRICT_ENV", "true")
    cfg = _reload_config(monkeypatch)
    assert cfg.validate_required_env() == []


def test_validate_required_env_strict_raises(monkeypatch):
    monkeypatch.delenv("SUPABASE_URL", raising=False)
    monkeypatch.setenv("SUPABASE_KEY", "test")
    monkeypatch.setenv("STRICT_ENV", "true")
    cfg = _reload_config(monkeypatch)
    with pytest.raises(RuntimeError) as exc:
        cfg.validate_required_env()
    assert "SUPABASE_URL" in str(exc.value)


def test_validate_required_env_lenient_warns(monkeypatch):
    monkeypatch.delenv("SUPABASE_URL", raising=False)
    monkeypatch.setenv("SUPABASE_KEY", "test")
    monkeypatch.setenv("STRICT_ENV", "false")
    cfg = _reload_config(monkeypatch)
    missing = cfg.validate_required_env()
    assert "SUPABASE_URL" in missing


def test_bool_env(monkeypatch):
    cfg = _reload_config(monkeypatch)
    for truthy in ("1", "true", "True", "yes", "on"):
        monkeypatch.setenv("FOO_FLAG", truthy)
        assert cfg._bool_env("FOO_FLAG") is True
    for falsy in ("0", "false", "no", "off", ""):
        monkeypatch.setenv("FOO_FLAG", falsy)
        assert cfg._bool_env("FOO_FLAG") is False


def test_cors_default_is_wildcard(monkeypatch):
    monkeypatch.delenv("CORS_ALLOW_ORIGINS", raising=False)
    cfg = _reload_config(monkeypatch)
    assert cfg.CORS_ALLOW_ORIGINS == ["*"]


def test_cors_allowlist_split(monkeypatch):
    monkeypatch.setenv(
        "CORS_ALLOW_ORIGINS",
        "https://app.example.com, https://staging.example.com",
    )
    cfg = _reload_config(monkeypatch)
    assert cfg.CORS_ALLOW_ORIGINS == [
        "https://app.example.com",
        "https://staging.example.com",
    ]
