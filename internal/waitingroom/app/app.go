package app

import (
	"github.com/leandersteiner/go-waiting-room/internal/waitingroom/app/command"
	"github.com/leandersteiner/go-waiting-room/internal/waitingroom/app/query"
	"github.com/redis/go-redis/v9"
)

type App struct {
	Commands Commands
	Queries  Queries
}

type Commands struct {
	JoinRoom            command.JoinRoomHandler
	IssueAdmissionToken command.IssueAdmissionTokenHandler
}

type Queries struct {
	RoomStatus query.RoomStatusHandler
	RoomStream query.RoomStreamHandler
}

func New(rdb *redis.Client) *App {
	return &App{
		Commands: Commands{
			JoinRoom:            command.NewJoinRoomHandler(rdb),
			IssueAdmissionToken: command.NewIssueAdmissionTokenHandler(rdb),
		},
		Queries: Queries{
			RoomStatus: query.NewRoomStatusHandler(rdb),
			RoomStream: query.NewRoomStreamHandler(),
		},
	}
}
