# Go Waiting Room

## Architecture

![architecture](docs/architecture.svg)

The queue service assigns each session a monotonically increasing arrival number
in Redis. The worker service uses a Redis leader lock so only one worker advances
`admitted_counter` for each active room on each tick. Queue position is computed
as:

```text
position = session_arrival_number - admitted_counter
```

When `position <= 0`, the session can request an admission token.

## Running Locally

Start Redis:

```bash
docker run --rm --name waitroom-redis -p 6379:6379 -d redis
```

Optionally configure a room. Missing config falls back to an enabled queue with
`admission_rate=250`, `max_active_admissions=250`, and
`token_ttl_seconds=900`.

```bash
redis-cli HSET waitroom:load:main:config \
  queue_enabled true \
  admission_rate 250 \
  max_active_admissions 500 \
  admission_offer_ttl_seconds 60 \
  token_ttl_seconds 900 \
  version 1
```

See [docs/admission-leases.md](docs/admission-leases.md) for the active lease
model and release API.

Admission tokens are Ed25519-signed JWTs. Downstream services can verify them
without calling the waiting room by caching the JWKS from:

```text
GET /.well-known/jwks.json
```

For production, configure a persistent Ed25519 signing key. The value can be a
base64-encoded 32-byte seed or a 64-byte Go Ed25519 private key. If omitted, the
queue service generates an ephemeral development key on startup.

```bash
openssl rand -base64 32
```

Start both services:

```bash
go run ./cmd/queue
go run ./cmd/worker
```

Useful environment variables:

```text
REDIS_ADDR=localhost:6379
REDIS_PASSWORD=
REDIS_DB=0
QUEUE_ADDR=:8080
ADMISSION_TOKEN_PRIVATE_KEY_BASE64=
ADMISSION_TOKEN_KEY_ID=
ADMISSION_TOKEN_ISSUER=go-waiting-room
ADMISSION_TOKEN_AUDIENCE=admission
WORKER_ID=<hostname:pid by default>
WORKER_TICK_INTERVAL=1s
WORKER_LOCK_TTL=5s
```
