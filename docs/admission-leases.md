# Admission Leases

The waiting room limits active ordering capacity with admission leases. This is
different from a pure admission-rate limiter.

An admission rate answers: how many users may be admitted per second?

An admission lease answers: how many users may be inside the ordering flow at
the same time?

For ordering, the lease is the source of truth. A user can only hold an ordering
slot after `/queue/token` creates an active admission lease. The signed JWT can
still be verified locally by downstream services; the waiting room is only
needed for lifecycle events such as releasing capacity.

## Room Config

Room config is stored in Redis at:

```text
waitroom:{tenantID}:{eventID}:config
```

Supported fields:

```text
queue_enabled                    true|false
admission_rate                   users admitted per second as offers
max_active_admissions            maximum concurrent active ordering leases
admission_offer_ttl_seconds      how long an admission offer is reserved
token_ttl_seconds                active lease/JWT lifetime
version                          config version
```

Defaults:

```text
queue_enabled=true
admission_rate=250
max_active_admissions=250
admission_offer_ttl_seconds=60
token_ttl_seconds=900
```

Example:

```bash
redis-cli HSET waitroom:tenant-1:event-1:config \
  queue_enabled true \
  admission_rate 50 \
  max_active_admissions 500 \
  admission_offer_ttl_seconds 60 \
  token_ttl_seconds 900 \
  version 1
```

## Flow

1. A user joins the room and receives an arrival number.
2. The worker holds the Redis leader lock and ticks over active rooms.
3. On each tick, the worker removes expired offers and expired active leases.
4. The worker calculates available capacity:

```text
available = max_active_admissions - active_admissions - admission_offers
```

5. The worker creates admission offers up to the lowest of:

```text
admission_rate budget for this tick
available capacity
queued users not yet offered
```

6. A client whose session is offered receives `canEnter=true` over SSE/status.
7. The client calls `/queue/token`.
8. `/queue/token` atomically consumes the offer and creates an active lease.
9. Downstream services verify the JWT locally using `/.well-known/jwks.json`.
10. When ordering finishes or is cancelled, downstream releases the lease.
11. If release is not called, the lease expiry score lets the worker free the
    slot after `token_ttl_seconds`.

## Redis Keys

The implementation keeps Redis memory bounded by active capacity, not total
waiting users.

```text
waitroom:{tenantID}:{eventID}:arrival_counter
waitroom:{tenantID}:{eventID}:admitted_counter
waitroom:{tenantID}:{eventID}:session:{sessionID}
waitroom:{tenantID}:{eventID}:admission_offers
waitroom:{tenantID}:{eventID}:active_admissions
waitroom:{tenantID}:{eventID}:token_issued:{sessionID}
waitroom:{tenantID}:{eventID}:session_lease:{sessionID}
waitroom:{tenantID}:{eventID}:lease_session:{tokenID}
```

`admission_offers` is a sorted set keyed by arrival number with an expiry score.
`active_admissions` is a sorted set keyed by JWT `jti` with an expiry score.

## Token Verification

Admission tokens are Ed25519-signed JWTs. Downstream services should cache:

```text
GET /.well-known/jwks.json
```

They should verify:

```text
signature
kid
iss
aud
exp
nbf
iat
scope=admission
tenant_id
event_id
jti
sub
```

The `jti` claim is the admission lease ID. The `sub` claim is the session ID.

## Release API

Downstream services should release capacity when the user completes, cancels, or
loses the reservation.

```http
POST /v1/tenants/{tenantID}/events/{eventID}/queue/admission/release
Content-Type: application/json

{
  "SessionID": "session-123",
  "TokenID": "jwt-jti-value"
}
```

The release operation is idempotent. A successful response has:

```json
{"Released": true}
```

If the lease was already released or expired, `Released` is `false`.

## Failure Behavior

The `/queue/token` endpoint is the final concurrency gate. A stale client may
still have seen `canEnter=true`, but token issuance only succeeds if there is a
valid offer or a free active slot. This prevents the number of active ordering
flows from exceeding `max_active_admissions`.

If downstream cannot call the release endpoint, capacity is held until the JWT
and active lease expire and the worker cleans the expired lease. For the
ordering flow, keep `token_ttl_seconds` aligned with the reservation window,
currently 15 minutes.
