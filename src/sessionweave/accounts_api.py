"""REST routes for broker accounts: admin management and caller self-service.

Admin routes (``/v1/accounts...``) take the relay admin token as Bearer.
Self routes (``/v1/me...``) take an account token. Both answer 503
``accounts_unavailable`` on a relay without a persistence DB.
"""
from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from sessionweave.access import admin_matches
from sessionweave.accounts import Account, AccountStore
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.http_api import (
    _bearer,
    _deps,
    _error_response,
    _read_json_body,
    _request_source_ip,
)
from sessionweave.tools import ToolDeps, resolve_account

logger = logging.getLogger(__name__)

NAME_MAX_LEN = 128
CONTACT_MAX_LEN = 256


def _store(deps: ToolDeps) -> AccountStore:
    if deps.accounts is None:
        raise ToolError(ErrorCode.ACCOUNTS_UNAVAILABLE, "accounts are not enabled on this relay")
    return deps.accounts


async def _cut_streams(
    request: Request, account_id: str, token_id: str | None = None, *, reason: str
) -> None:
    """End live firehose streams opened with the account's (revoked) token(s)."""
    firehose = getattr(request.app.state, "firehose", None)
    if firehose is not None:
        await firehose.close_for_account(account_id, token_id=token_id, reason=reason)


def _unauthorized() -> JSONResponse:
    return JSONResponse(
        {"error": {"code": "unauthorized", "message": "missing Authorization: Bearer header"}},
        status_code=401,
    )


def _require_admin(request: Request, deps: ToolDeps) -> JSONResponse | None:
    """None when the caller is the admin; otherwise the error response."""
    token = _bearer(request)
    if not token:
        return _unauthorized()
    source_ip = _request_source_ip(request)
    if admin_matches(token, deps.admin_token):
        return None
    if deps.auth_limiter.is_throttled("", source_ip):
        return _error_response(ToolError(ErrorCode.RATE_LIMITED, "too many failed attempts"))
    deps.auth_limiter.record_failure("", source_ip)
    return _error_response(
        ToolError(ErrorCode.PERMISSION_DENIED, "the relay admin token is required")
    )


async def _require_self(request: Request, deps: ToolDeps) -> Account:
    token = _bearer(request)
    if not token:
        raise ToolError(ErrorCode.INVALID_ACCOUNT_TOKEN, "missing Authorization: Bearer header")
    return await resolve_account(deps, token, source_ip=_request_source_ip(request))


def _bad(message: str) -> ToolError:
    return ToolError(ErrorCode.INVALID_REQUEST, message)


async def _body(request: Request) -> dict[str, Any]:
    """The JSON object body; malformed or non-object bodies are invalid_request."""
    try:
        return await _read_json_body(request)
    except ToolError as exc:
        if exc.code is ErrorCode.INVALID_CONFIG:
            raise _bad(exc.message) from exc
        raise


def _opt_str(body: dict[str, Any], key: str) -> str | None:
    value = body.get(key)
    if value is not None and not isinstance(value, str):
        raise _bad(f"{key} must be a string")
    if key == "contact" and value is not None and len(value) > CONTACT_MAX_LEN:
        raise _bad(f"contact must be at most {CONTACT_MAX_LEN} characters")
    return value


def _name(value: str | None) -> str | None:
    if value is not None and not 1 <= len(value.strip()) <= NAME_MAX_LEN:
        raise _bad(f"name must be 1-{NAME_MAX_LEN} characters")
    return value.strip() if value is not None else None


def _expires_ms(body: dict[str, Any]) -> int | None:
    raw = _opt_str(body, "expires_at")
    if raw is None:
        return None
    try:
        when = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise _bad("expires_at must be an ISO 8601 timestamp") from exc
    if when.tzinfo is None:
        when = when.replace(tzinfo=UTC)
    ms = int(when.timestamp() * 1000)
    if ms <= int(datetime.now(UTC).timestamp() * 1000):
        raise _bad("expires_at must be in the future")
    return ms


async def _account_with_tokens(store: AccountStore, account: Account) -> dict[str, Any]:
    tokens = await store.list_tokens(account.account_id)
    return {**account.to_public(), "tokens": [t.to_public() for t in tokens]}


async def _mint_token(
    deps: ToolDeps, store: AccountStore, account_id: str, body: dict[str, Any]
) -> JSONResponse:
    label = body.get("label")
    if not isinstance(label, str):
        raise _bad("label is required")
    plaintext, meta = await store.create_token(
        account_id, label, expires_at_ms=_expires_ms(body),
        max_tokens=deps.account_max_tokens,
    )
    return JSONResponse(
        {
            "token": plaintext,
            "token_id": meta.token_id,
            "label": meta.label,
            "created_at": meta.created_at,
            "expires_at": meta.expires_at,
        },
        status_code=201,
    )


# ---------- admin routes ----------


async def create_account(request: Request) -> Response:
    deps = _deps(request)
    try:
        store = _store(deps)
        if (denied := _require_admin(request, deps)) is not None:
            return denied
        body = await _body(request)
        name = _name(body.get("name") if isinstance(body.get("name"), str) else None)
        if name is None:
            raise _bad("name is required")
        status = body.get("status", "active")
        created_by = body.get("created_by", "admin")
        if not isinstance(status, str) or not isinstance(created_by, str):
            raise _bad("status and created_by must be strings")
        account = await store.create(
            name, status=status, contact=_opt_str(body, "contact"), created_by=created_by
        )
        return JSONResponse(account.to_public(), status_code=201)
    except ToolError as exc:
        return _error_response(exc)


async def list_accounts(request: Request) -> Response:
    deps = _deps(request)
    try:
        store = _store(deps)
        if (denied := _require_admin(request, deps)) is not None:
            return denied
        return JSONResponse(
            {"accounts": [a.to_public() for a in await store.list_accounts()]}
        )
    except ToolError as exc:
        return _error_response(exc)


async def get_account(request: Request) -> Response:
    deps = _deps(request)
    try:
        store = _store(deps)
        if (denied := _require_admin(request, deps)) is not None:
            return denied
        account = await store.get(request.path_params["account_id"])
        return JSONResponse(await _account_with_tokens(store, account))
    except ToolError as exc:
        return _error_response(exc)


async def patch_account(request: Request) -> Response:
    """Update an account. A status change takes effect at once for its tokens;
    its sessions follow: ``disabled`` suspends them, ``active`` lifts the
    suspension (no continue is sent); both idempotent,
    so repeating the PATCH repairs a partial failure."""
    deps = _deps(request)
    try:
        store = _store(deps)
        if (denied := _require_admin(request, deps)) is not None:
            return denied
        account_id = request.path_params["account_id"]
        body = await _body(request)
        status = _opt_str(body, "status")
        _, after = await store.update(
            account_id,
            status=status,
            name=_name(_opt_str(body, "name")),
            contact=_opt_str(body, "contact"),
        )
        if after.status != "active":
            # Cut first: a failed (fail-closed) suspend must not leave streams open.
            await _cut_streams(request, account_id, reason="account not active")
        affected = 0
        if status == "disabled":
            affected = await deps.registry.suspend_owned(account_id)
        elif status == "active":
            affected = await deps.registry.resume_owned(account_id)
        return JSONResponse({**after.to_public(), "sessions_affected": affected})
    except ToolError as exc:
        return _error_response(exc)


async def delete_account(request: Request) -> Response:
    """Delete an account with all its sessions; kept if any session cannot go."""
    deps = _deps(request)
    try:
        store = _store(deps)
        if (denied := _require_admin(request, deps)) is not None:
            return denied
        account_id = request.path_params["account_id"]
        await store.get(account_id)
        # Disable first: its tokens stop at once, so no new session can be
        # created while the existing ones are deleted.
        await store.update(account_id, status="disabled")
        await _cut_streams(request, account_id, reason="account deleted")
        try:
            deleted = await deps.registry.delete_owned(account_id)
        except ToolError as exc:
            if exc.code is ErrorCode.PERSISTENCE_FAILED:
                raise ToolError(
                    ErrorCode.PERSISTENCE_FAILED,
                    f"account disabled but kept; {exc.message}; retry the delete",
                ) from exc
            raise
        try:
            await store.delete(account_id)
        except ToolError:
            raise
        except Exception as exc:
            logger.exception("could not delete account %s", account_id)
            raise ToolError(
                ErrorCode.PERSISTENCE_FAILED,
                "account disabled and its sessions deleted, but the account row "
                "could not be removed; retry the delete",
            ) from exc
        return JSONResponse({"deleted_sessions": deleted})
    except ToolError as exc:
        return _error_response(exc)


async def create_account_token(request: Request) -> Response:
    deps = _deps(request)
    try:
        store = _store(deps)
        if (denied := _require_admin(request, deps)) is not None:
            return denied
        return await _mint_token(deps, store, request.path_params["account_id"],
                                 await _body(request))
    except ToolError as exc:
        return _error_response(exc)


async def revoke_account_token(request: Request) -> Response:
    deps = _deps(request)
    try:
        store = _store(deps)
        if (denied := _require_admin(request, deps)) is not None:
            return denied
        account_id = request.path_params["account_id"]
        await store.get(account_id)
        token_id = request.path_params["token_id"]
        await store.revoke_token(account_id, token_id)
        await _cut_streams(request, account_id, token_id, reason="account token revoked")
        return Response(status_code=204)
    except ToolError as exc:
        return _error_response(exc)


# ---------- self routes ----------


async def get_me(request: Request) -> Response:
    deps = _deps(request)
    try:
        store = _store(deps)
        account = await _require_self(request, deps)
        return JSONResponse(await _account_with_tokens(store, account))
    except ToolError as exc:
        return _error_response(exc)


async def list_my_tokens(request: Request) -> Response:
    deps = _deps(request)
    try:
        store = _store(deps)
        account = await _require_self(request, deps)
        tokens = await store.list_tokens(account.account_id)
        return JSONResponse({"tokens": [t.to_public() for t in tokens]})
    except ToolError as exc:
        return _error_response(exc)


async def create_my_token(request: Request) -> Response:
    deps = _deps(request)
    try:
        store = _store(deps)
        account = await _require_self(request, deps)
        return await _mint_token(deps, store, account.account_id, await _body(request))
    except ToolError as exc:
        return _error_response(exc)


async def revoke_my_token(request: Request) -> Response:
    deps = _deps(request)
    try:
        store = _store(deps)
        account = await _require_self(request, deps)
        token_id = request.path_params["token_id"]
        await store.revoke_token(account.account_id, token_id)
        await _cut_streams(
            request, account.account_id, token_id, reason="account token revoked"
        )
        return Response(status_code=204)
    except ToolError as exc:
        return _error_response(exc)


async def list_my_sessions(request: Request) -> Response:
    deps = _deps(request)
    try:
        _store(deps)
        account = await _require_self(request, deps)
        return JSONResponse(
            {"sessions": await deps.registry.owned_session_summaries(account.account_id)}
        )
    except ToolError as exc:
        return _error_response(exc)


def routes() -> list[Route]:
    return [
        Route("/v1/accounts", create_account, methods=["POST"]),
        Route("/v1/accounts", list_accounts, methods=["GET"]),
        Route("/v1/accounts/{account_id}", get_account, methods=["GET"]),
        Route("/v1/accounts/{account_id}", patch_account, methods=["PATCH"]),
        Route("/v1/accounts/{account_id}", delete_account, methods=["DELETE"]),
        Route("/v1/accounts/{account_id}/tokens", create_account_token, methods=["POST"]),
        Route(
            "/v1/accounts/{account_id}/tokens/{token_id}",
            revoke_account_token, methods=["DELETE"],
        ),
        Route("/v1/me", get_me, methods=["GET"]),
        Route("/v1/me/tokens", list_my_tokens, methods=["GET"]),
        Route("/v1/me/tokens", create_my_token, methods=["POST"]),
        Route("/v1/me/tokens/{token_id}", revoke_my_token, methods=["DELETE"]),
        Route("/v1/me/sessions", list_my_sessions, methods=["GET"]),
    ]
