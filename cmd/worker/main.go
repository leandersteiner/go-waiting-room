package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/leandersteiner/go-waiting-room/internal/waitingroom/repository"
	waitingroomworker "github.com/leandersteiner/go-waiting-room/internal/waitingroom/worker"
	"github.com/redis/go-redis/v9"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(&redis.Options{
		Addr:     env("REDIS_ADDR", "localhost:6379"),
		Password: env("REDIS_PASSWORD", ""),
		DB:       envInt("REDIS_DB", 0),
	})
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("ping redis: %s", err)
	}

	ownerID, err := workerID()
	if err != nil {
		log.Fatalf("build worker id: %s", err)
	}

	repo := repository.NewRedisRepository(rdb)
	worker, err := waitingroomworker.New(repo, waitingroomworker.Config{
		OwnerID:      ownerID,
		TickInterval: envDuration("WORKER_TICK_INTERVAL", time.Second),
		LockTTL:      envDuration("WORKER_LOCK_TTL", 5*time.Second),
		Logger:       log.Default(),
	})
	if err != nil {
		log.Fatalf("create worker: %s", err)
	}

	log.Printf("worker started owner=%s redis=%s", ownerID, rdb.Options().Addr)
	if err := worker.Run(ctx); err != nil {
		log.Fatalf("run worker: %s", err)
	}
}

func workerID() (string, error) {
	if value := env("WORKER_ID", ""); value != "" {
		return value, nil
	}

	hostname, err := os.Hostname()
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s:%d", hostname, os.Getpid()), nil
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}
