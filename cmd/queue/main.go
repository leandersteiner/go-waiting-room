package main

import (
	"context"
	"net/http"
	"time"

	waitingroomapp "github.com/leandersteiner/go-waiting-room/internal/waitingroom/app"
	"github.com/leandersteiner/go-waiting-room/internal/waitingroom/server"
	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		panic(err)
	}

	waitingroomApp := waitingroomapp.New(rdb)
	waitingroomServer := server.NewHTTPServer(waitingroomApp)
	err := waitingroomServer.Run(&http.Server{
		Addr:         ":8080",
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  5 * time.Minute,
	})
	if err != nil {
		panic(err)
	}
}
