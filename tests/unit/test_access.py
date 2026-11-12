from __future__ import annotations

import pytest

from sessionweave.access import (
    JOIN_TOKEN_EXPIRED_GRACE_MS,
    JOIN_TOKEN_TTL_SECONDS,
    AuthFailureLimiter,
    IssuedToken,
    Principal,
    SessionAuth,
    admin_matches,
    generate_token,
    hash_token,
)


def _auth() -> tuple[SessionAuth, str]:
    creator = generate_token()
    return SessionAuth(hash_token(creator)), creator


def test_ttl_constant() -> None:
    assert JOIN_TOKEN_TTL_SECONDS == 900


def test_join_token_valid_for_15_minutes() -> None:
    auth, _ = _auth()
    tok, issued = auth.mint_join(0)
    assert issued.kind == "join"
    assert issued.expires_at_ms == 900_000
    m = auth.match(tok, 899_999)
    assert m is not None and m.role == "joiner"
    assert auth.match(tok, 900_000) is None
    assert auth.join_token_expired(tok, 900_000) is True
    assert auth.join_token_expired(tok, 899_999) is False
    assert auth.join_token_expired("unknown", 900_000) is False


def test_join_token_multi_use() -> None:
    auth, _ = _auth()
    tok, _ = auth.mint_join(0)
    assert auth.match(tok, 1_000) == Principal("joiner")
    assert auth.match(tok, 2_000) == Principal("joiner")


def test_creator_matches_before_others() -> None:
    auth, creator = _auth()
    assert auth.match(creator, 0) == Principal("creator")
    assert auth.match("nope", 0) is None


def test_no_creator_hash_never_matches_creator() -> None:
    auth = SessionAuth(None)
    assert auth.match("anything", 0) is None


def test_supervisor_revoked_no_longer_matches() -> None:
    auth, _ = _auth()
    tok, issued = auth.mint_supervisor(10)
    assert issued.kind == "supervisor" and issued.expires_at_ms is None
    assert auth.match(tok, 20) == Principal("supervisor")
    revoked = auth.revoke_supervisor(issued.token_id, 30)
    assert revoked.revoked_at_ms == 30
    assert auth.match(tok, 40) is None
    assert [t.token_id for t in auth.supervisor_tokens()] == [issued.token_id]


def test_revoke_unknown_raises() -> None:
    auth, _ = _auth()
    with pytest.raises(KeyError):
        auth.revoke_supervisor("missing", 0)


def test_participant_matches_with_its_id() -> None:
    auth, _ = _auth()
    p_tok = generate_token()
    auth.register_participant("p1", hash_token(p_tok))
    assert auth.match(p_tok, 0) == Principal("participant", "p1")
    auth.forget_participant("p1")
    assert auth.match(p_tok, 0) is None


def test_participant_id_is_not_a_token() -> None:
    auth, _ = _auth()
    auth.register_participant("p1", hash_token("secret"))
    assert auth.match("p1", 0) is None


def test_restore_round_trip() -> None:
    auth, creator = _auth()
    j_tok, j = auth.mint_join(0)
    s_tok, s = auth.mint_supervisor(0)
    auth.revoke_supervisor(s.token_id, 5)
    fresh = SessionAuth(hash_token(creator))
    fresh.restore(
        [
            IssuedToken(
                t.token_id,
                t.kind,
                t.token_hash,
                t.created_at_ms,
                t.expires_at_ms,
                t.revoked_at_ms,
            )
            for t in (j, s)
        ]
    )
    assert fresh.match(j_tok, 899_999) == Principal("joiner")
    assert fresh.match(j_tok, 900_000) is None
    assert fresh.match(s_tok, 10) is None
    assert fresh.match(creator, 0) == Principal("creator")
    assert [t.token_id for t in fresh.supervisor_tokens()] == [s.token_id]


def test_limiter_keyed_by_sid_and_ip() -> None:
    now = [0.0]
    lim = AuthFailureLimiter(clock=lambda: now[0])
    for _ in range(5):
        lim.record_failure("s", "ip1")
    assert lim.is_throttled("s", "ip1") is True
    assert lim.is_throttled("s", "ip2") is False
    assert lim.is_throttled("s2", "ip1") is False
    now[0] = 61.0
    assert lim.is_throttled("s", "ip1") is False


def test_admin_matches_unset_is_false() -> None:
    assert admin_matches("x", None) is False
    assert admin_matches("x", "") is False
    assert admin_matches("x", "x") is True
    assert admin_matches("y", "x") is False


def test_lone_surrogate_does_not_crash() -> None:
    auth, _ = _auth()
    assert isinstance(hash_token("\ud800"), bytes)
    assert auth.match("\ud800", 0) is None
    assert admin_matches("\ud800", "x") is False
    assert admin_matches("\ud800", "\ud800") is True


def test_limiter_below_threshold_not_throttled() -> None:
    lim = AuthFailureLimiter(clock=lambda: 0.0)
    for _ in range(4):
        lim.record_failure("s", "ip")
    assert lim.is_throttled("s", "ip") is False


def test_limiter_window_boundary() -> None:
    now = [0.0]
    lim = AuthFailureLimiter(clock=lambda: now[0])
    for _ in range(5):
        lim.record_failure("s", "ip")
    now[0] = 60.0
    assert lim.is_throttled("s", "ip") is True
    now[0] = 60.001
    assert lim.is_throttled("s", "ip") is False


def test_limiter_bucket_capped_and_stale_buckets_dropped() -> None:
    now = [0.0]
    lim = AuthFailureLimiter(clock=lambda: now[0])
    for _ in range(50):
        lim.record_failure("s", "ip")
    assert len(lim._buckets[("s", "ip")]) == 5
    for i in range(10):
        lim.record_failure(f"s{i}", "ip")
    now[0] = 1000.0
    for i in range(300):
        lim.record_failure("fresh", f"ip{i}")
    assert ("s", "ip") not in lim._buckets
    assert ("s0", "ip") not in lim._buckets


def test_mint_join_prunes_long_expired_join_tokens() -> None:
    auth, _ = _auth()
    old, old_issued = auth.mint_join(0)
    recent, _ = auth.mint_join(1_000_000)
    sup, _ = auth.mint_supervisor(0)
    # Just past expiry the old link still reads as "expired" (friendly message).
    auth.mint_join(900_001)
    assert auth.join_token_expired(old, 900_001) is True
    # Long after expiry, the next mint drops it from memory.
    later = 900_000 + JOIN_TOKEN_EXPIRED_GRACE_MS + 1
    auth.mint_join(later)
    assert old_issued.token_hash not in auth._issued
    assert auth.join_token_expired(old, later) is False
    assert hash_token(recent) in auth._issued  # expired less than the grace ago
    assert auth.match(sup, later) == Principal("supervisor")
