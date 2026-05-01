package main

import (
	"net/http"
	"time"

	waitingroomapp "github.com/leandersteiner/go-waiting-room/internal/waitingroom/app"
	"github.com/leandersteiner/go-waiting-room/internal/waitingroom/server"
)

func main() {
	waitingroomApp := waitingroomapp.New()
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
