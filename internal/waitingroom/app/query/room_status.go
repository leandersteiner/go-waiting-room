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

type SessionStatusStore interface {
	GetSessionStatus(ctx context.Context, tenantID string, eventID string, sessionID string) (waitingroom.SessionStatus, error)
}

type roomStatusHandler struct {
	store SessionStatusStore
}

func NewRoomStatusHandler(store SessionStatusStore) RoomStatusHandler {
	return (&roomStatusHandler{store: store}).Handle
}

func (h *roomStatusHandler) Handle(ctx context.Context, query RoomStatus) (RoomStatusResponse, error) {
	status, err := h.store.GetSessionStatus(ctx, query.TenantID, query.EventID, query.SessionID)
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
