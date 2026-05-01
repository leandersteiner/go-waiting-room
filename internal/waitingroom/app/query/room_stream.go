package query

import "context"

type RoomStream struct {
	SessionID string
}

type RoomStreamHandler func(ctx context.Context, query RoomStream) error

type roomStreamHandler struct{}

func NewRoomStreamHandler() RoomStreamHandler {
	return (&roomStreamHandler{}).Handle
}

func (h *roomStreamHandler) Handle(ctx context.Context, query RoomStream) error {
	return nil
}
