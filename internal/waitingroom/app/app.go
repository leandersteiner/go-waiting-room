package app

import (
	"github.com/leandersteiner/go-waiting-room/internal/waitingroom"
	"github.com/leandersteiner/go-waiting-room/internal/waitingroom/app/command"
	"github.com/leandersteiner/go-waiting-room/internal/waitingroom/app/query"
)

type App struct {
	Commands           Commands
	Queries            Queries
	AdmissionTokenKeys waitingroom.AdmissionTokenKeySetProvider
}

type Commands struct {
	JoinRoom            command.JoinRoomHandler
	IssueAdmissionToken command.IssueAdmissionTokenHandler
	ReleaseAdmission    command.ReleaseAdmissionHandler
}

type Queries struct {
	RoomStatus       query.RoomStatusHandler
	StreamRoomStatus query.StreamRoomStatusHandler
}

type Repository interface {
	command.JoinRoomStore
	command.AdmissionTokenStore
	command.AdmissionReleaseStore
	query.SessionStatusStore
}

func New(repo Repository) *App {
	keySetProvider, _ := repo.(waitingroom.AdmissionTokenKeySetProvider)

	return &App{
		Commands: Commands{
			JoinRoom:            command.NewJoinRoomHandler(repo),
			IssueAdmissionToken: command.NewIssueAdmissionTokenHandler(repo),
			ReleaseAdmission:    command.NewReleaseAdmissionHandler(repo),
		},
		Queries: Queries{
			RoomStatus:       query.NewRoomStatusHandler(repo),
			StreamRoomStatus: query.NewStreamRoomStatusHandler(repo),
		},
		AdmissionTokenKeys: keySetProvider,
	}
}
