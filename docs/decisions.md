# Decisions

This is a list of decisions made for this project.

## Technologies

1. Golang
2. Redis
3. SSE

## Deployments

### Queue Service

The queue service is the heart of the waiting room.
It accepts users and queues them up in the waiting room.
It updates the queued clients through SSE and returns
an admission token to the user once they are admitted.

### Worker Service

The worker service is a background worker that manages waiting room configuration
and admission of users on every tick.

The problem is that admission can only happen inside one pod.
The easiest solution is to enforce a single replica.
This would create a single point of failure and would mean that no one gets
admission if the pod goes down until k8s creates a new one.
The simplest solution for now is to use redis for a simple leader election.

All workers try to get a lock from redis. Only the one who gets the lock is allowed
to admit users. The lock has a TTL so if the current leader crashes, another worker
will take over within the defined TTL.