package command

import "context"

type JoinRoom struct {
	SessionID string
}

type JoinRoomResponse struct {
	ArrivalNumber          int
	Ahead                  int
	EstimatedWaitInSeconds int
	QueueEnabled           bool
}

type JoinRoomHandler func(ctx context.Context, cmd JoinRoom) (JoinRoomResponse, error)

type joinRoomHandler struct{}

func NewJoinRoomHandler() JoinRoomHandler {
	return (&joinRoomHandler{}).Handle
}

func (h *joinRoomHandler) Handle(ctx context.Context, cmd JoinRoom) (JoinRoomResponse, error) {
	return JoinRoomResponse{}, nil
}
