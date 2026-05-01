package query

import "context"

type RoomStatus struct {
	TenantID  string
	EventID   string
	SessionID string
}

type RoomStatusResponse struct {
	Ahead                  int
	EstimatedWaitInSeconds int
}

type RoomStatusHandler func(ctx context.Context, query RoomStatus) (RoomStatusResponse, error)

type roomStatusHandler struct{}

func NewRoomStatusHandler() RoomStatusHandler {
	return (&roomStatusHandler{}).Handle
}

func (h *roomStatusHandler) Handle(ctx context.Context, query RoomStatus) (RoomStatusResponse, error) {
	return RoomStatusResponse{}, nil
}
