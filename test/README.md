# Load Tests

Start Redis plus both services first:

```bash
docker run --rm --name waitroom-redis -p 6379:6379 -d redis
go run ./cmd/queue
go run ./cmd/worker
```

Reset the load-test room between runs:

```bash
redis-cli EVAL "for _,k in ipairs(redis.call('KEYS', ARGV[1])) do redis.call('DEL', k) end" 0 'waitroom:load:*'
```

Run a ramping join test:

```bash
k6 run test/join-ramp.js
```

Run a sustained join test:

```bash
RATE=10000 DURATION=5m k6 run test/join-sustained.js
```

Run a join plus status read-after-write test. This issues two HTTP requests per iteration:

```bash
RATE=1000 DURATION=5m k6 run test/join-status-mix.js
```

Run the SSE connection holder:

```bash
go run ./test/sse-streams.go -sessions 10000 -hold 5m -open-rate 500
```

Run the full user flow:

```bash
go run ./test/e2e-flow -sessions 100 -timeout 10m -open-rate 500
```

The full flow joins every session, opens one SSE stream per session, waits for
`canEnter=true`, calls `/queue/token`, and closes that user's stream. Add
`-gate-streams` if you want every stream subscribed before any client processes
events:

```bash
go run ./test/e2e-flow -sessions 100 -timeout 10m -open-rate 500 -gate-streams
```

Queue progress advances in the worker through `admitted_counter`. The SSE
endpoint emits regular status updates every 5 seconds and also reacts to worker
admission progress notifications.

Watch Redis while tests are running:

```bash
redis-cli INFO commandstats
redis-cli INFO memory
redis-cli --latency
```
