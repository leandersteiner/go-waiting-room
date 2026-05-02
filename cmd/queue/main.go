package main

import (
	"context"
	"net/http"
	"os"
	"strconv"
	"time"

	waitingroomapp "github.com/leandersteiner/go-waiting-room/internal/waitingroom/app"
	"github.com/leandersteiner/go-waiting-room/internal/waitingroom/repository"
	"github.com/leandersteiner/go-waiting-room/internal/waitingroom/server"
	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     env("REDIS_ADDR", "localhost:6379"),
		Password: env("REDIS_PASSWORD", ""),
		DB:       envInt("REDIS_DB", 0),
	})
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		panic(err)
	}

	repo := repository.NewRedisRepository(rdb)
	app := waitingroomapp.New(repo)
	srv := server.NewHTTPServer(app)
	err := srv.Run(&http.Server{
		Addr:         env("QUEUE_ADDR", ":8080"),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  5 * time.Minute,
	})
	if err != nil {
		panic(err)
	}
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
