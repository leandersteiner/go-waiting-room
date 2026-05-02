package repository

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const workerLockKey = redisKeyPrefix + ":worker:lock"

func (r *RedisRepository) TryAcquireWorkerLock(ctx context.Context, owner string, ttl time.Duration) (bool, error) {
	if strings.TrimSpace(owner) == "" {
		return false, errors.New("worker lock owner is required")
	}
	if ttl <= 0 {
		return false, errors.New("worker lock ttl must be positive")
	}

	acquired, err := acquireWorkerLockScript.Run(
		ctx,
		r.rdb,
		[]string{workerLockKey},
		owner,
		strconv.FormatInt(ttl.Milliseconds(), 10),
	).Bool()
	if err != nil {
		return false, fmt.Errorf("acquire worker lock: %w", err)
	}

	return acquired, nil
}
