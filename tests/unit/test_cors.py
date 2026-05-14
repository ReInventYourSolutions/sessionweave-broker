"""Unit tests for the CORS configuration helper."""
from __future__ import annotations

from sessionweave.cors import CORSConfig


def test_default_is_wildcard():
    cfg = CORSConfig.from_env({})
    assert cfg.allow_origins == ["*"]
    assert cfg.allow_credentials is False


def test_explicit_csv_allowlist():
    cfg = CORSConfig.from_env({"SW_CORS_ORIGINS": "https://a.example,https://b.example"})
    assert cfg.allow_origins == ["https://a.example", "https://b.example"]


def test_explicit_single_origin():
    cfg = CORSConfig.from_env({"SW_CORS_ORIGINS": "https://only.example"})
    assert cfg.allow_origins == ["https://only.example"]


def test_blank_value_falls_back_to_wildcard():
    cfg = CORSConfig.from_env({"SW_CORS_ORIGINS": ""})
    assert cfg.allow_origins == ["*"]


def test_whitespace_in_csv_is_stripped():
    cfg = CORSConfig.from_env({"SW_CORS_ORIGINS": " https://a.example , https://b.example "})
    assert cfg.allow_origins == ["https://a.example", "https://b.example"]


def test_wildcard_forces_no_credentials():
    cfg = CORSConfig.from_env({"SW_CORS_ORIGINS": "*"})
    assert cfg.allow_credentials is False
