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
`admission_rate=50` and `token_ttl_seconds=300`.

```bash
redis-cli HSET waitroom:load:main:config queue_enabled true admission_rate 500 version 1
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
WORKER_ID=<hostname:pid by default>
WORKER_TICK_INTERVAL=1s
WORKER_LOCK_TTL=5s
```
