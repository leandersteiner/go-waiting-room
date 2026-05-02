package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/leandersteiner/go-waiting-room/internal/waitingroom"
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

	tokenIssuer, err := admissionTokenIssuerFromEnv()
	if err != nil {
		panic(err)
	}

	repo := repository.NewRedisRepository(rdb, repository.WithAdmissionTokenIssuer(tokenIssuer))
	app := waitingroomapp.New(repo)
	srv := server.NewHTTPServer(app)
	err = srv.Run(&http.Server{
		Addr:         env("QUEUE_ADDR", ":8080"),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  5 * time.Minute,
	})
	if err != nil {
		panic(err)
	}
}

func admissionTokenIssuerFromEnv() (waitingroom.AdmissionTokenIssuer, error) {
	keyID := env("ADMISSION_TOKEN_KEY_ID", "")
	issuer := env("ADMISSION_TOKEN_ISSUER", "")
	audience := env("ADMISSION_TOKEN_AUDIENCE", "")
	privateKeyValue := env("ADMISSION_TOKEN_PRIVATE_KEY_BASE64", "")

	if privateKeyValue == "" {
		log.Print("ADMISSION_TOKEN_PRIVATE_KEY_BASE64 is not set; generated admission tokens will use an ephemeral development key")
		return waitingroom.NewGeneratedJWTAdmissionTokenIssuer(keyID, issuer, audience)
	}

	privateKey, err := waitingroom.ParseEd25519PrivateKey(privateKeyValue)
	if err != nil {
		return nil, err
	}

	return waitingroom.NewJWTAdmissionTokenIssuer(privateKey, keyID, issuer, audience)
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
