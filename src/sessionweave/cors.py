"""CORS configuration for the relay.

Default is wildcard. Safe because the relay's auth model is bearer-only
on every request — no cookies, ``allow_credentials`` is always False.
Operators of private deployments who want to restrict can set
``SW_CORS_ORIGINS`` to an explicit comma-separated list.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware

_DEFAULT_ORIGINS = ["*"]
_ALLOW_METHODS = ["GET", "POST", "DELETE", "PATCH", "OPTIONS"]
_ALLOW_HEADERS = ["authorization", "content-type"]


@dataclass(frozen=True)
class CORSConfig:
    allow_origins: list[str]
    allow_credentials: bool = False

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> "CORSConfig":
        raw = (env.get("SW_CORS_ORIGINS") or "").strip()
        if not raw:
            return cls(allow_origins=list(_DEFAULT_ORIGINS))
        origins = [piece.strip() for piece in raw.split(",") if piece.strip()]
        if not origins:
            return cls(allow_origins=list(_DEFAULT_ORIGINS))
        return cls(allow_origins=origins)

    def as_middleware(self) -> Middleware:
        return Middleware(
            CORSMiddleware,
            allow_origins=self.allow_origins,
            allow_credentials=self.allow_credentials,
            allow_methods=_ALLOW_METHODS,
            allow_headers=_ALLOW_HEADERS,
        )
