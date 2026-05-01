package app

import (
	"github.com/leandersteiner/go-waiting-room/internal/waitingroom/app/command"
	"github.com/leandersteiner/go-waiting-room/internal/waitingroom/app/query"
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

func New() *App {
	return &App{
		Commands: Commands{
			JoinRoom:            command.NewJoinRoomHandler(),
			IssueAdmissionToken: command.NewIssueAdmissionTokenHandler(),
		},
		Queries: Queries{
			RoomStatus: query.NewRoomStatusHandler(),
			RoomStream: query.NewRoomStreamHandler(),
		},
	}
}
