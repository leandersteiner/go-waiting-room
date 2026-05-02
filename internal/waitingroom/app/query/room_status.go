package query

import (
	"context"

	"github.com/leandersteiner/go-waiting-room/internal/waitingroom"
)

type RoomStatus struct {
	TenantID  string
	EventID   string
	SessionID string
}

type RoomStatusResponse struct {
	ArrivalNumber          int
	Position               int
	Ahead                  int
	EstimatedWaitInSeconds int
	CanEnter               bool
}

type RoomStatusHandler func(ctx context.Context, query RoomStatus) (RoomStatusResponse, error)

type roomStatusHandler struct {
	repo waitingroom.Repository
}

func NewRoomStatusHandler(repo waitingroom.Repository) RoomStatusHandler {
	return (&roomStatusHandler{repo: repo}).Handle
}

func (h *roomStatusHandler) Handle(ctx context.Context, query RoomStatus) (RoomStatusResponse, error) {
	status, err := h.repo.GetSessionStatus(ctx, query.TenantID, query.EventID, query.SessionID)
	if err != nil {
		return RoomStatusResponse{}, err
	}

	return RoomStatusResponse{
		ArrivalNumber:          status.ArrivalNumber,
		Position:               status.Position,
		Ahead:                  status.Ahead,
		EstimatedWaitInSeconds: status.EstimatedWaitInSeconds,
		CanEnter:               status.CanEnter,
	}, nil
}
