package app

import (
	"github.com/leandersteiner/go-waiting-room/internal/waitingroom"
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
	RoomStatus       query.RoomStatusHandler
	StreamRoomStatus query.StreamRoomStatusHandler
}

func New(repo waitingroom.Repository) *App {
	return &App{
		Commands: Commands{
			JoinRoom:            command.NewJoinRoomHandler(repo),
			IssueAdmissionToken: command.NewIssueAdmissionTokenHandler(repo),
		},
		Queries: Queries{
			RoomStatus:       query.NewRoomStatusHandler(repo),
			StreamRoomStatus: query.NewStreamRoomStatusHandler(repo),
		},
	}
}
