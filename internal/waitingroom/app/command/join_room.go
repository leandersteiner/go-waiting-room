package command

import (
	"context"
	"errors"

	"github.com/leandersteiner/go-waiting-room/internal/waitingroom"
	"github.com/leandersteiner/go-waiting-room/internal/waitingroom/repository"
)

type JoinRoom struct {
	TenantID  string
	EventID   string
	SessionID string
}

type JoinRoomResponse struct {
	ArrivalNumber          int
	Ahead                  int
	EstimatedWaitInSeconds int
	QueueEnabled           bool
}

type JoinRoomHandler func(ctx context.Context, cmd JoinRoom) (JoinRoomResponse, error)

type joinRoomHandler struct {
	repo waitingroom.Repository
}

func NewJoinRoomHandler(repo waitingroom.Repository) JoinRoomHandler {
	return (&joinRoomHandler{repo: repo}).Handle
}

func (h *joinRoomHandler) Handle(ctx context.Context, cmd JoinRoom) (JoinRoomResponse, error) {
	status, err := h.repo.JoinRoom(ctx, cmd.TenantID, cmd.EventID, cmd.SessionID)
	if err != nil {
		if errors.Is(err, repository.ErrRoomNotFound) {
			return JoinRoomResponse{
				QueueEnabled: false,
			}, nil
		}
		return JoinRoomResponse{}, err
	}

	return JoinRoomResponse{
		ArrivalNumber:          status.ArrivalNumber,
		Ahead:                  status.Ahead,
		EstimatedWaitInSeconds: status.EstimatedWaitInSeconds,
		QueueEnabled:           true,
	}, err
}
