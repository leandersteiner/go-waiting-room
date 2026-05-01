package command

import (
	"context"
	"errors"
	"math"

	"github.com/redis/go-redis/v9"
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
	rdb *redis.Client
}

func NewJoinRoomHandler(rdb *redis.Client) JoinRoomHandler {
	return (&joinRoomHandler{rdb: rdb}).Handle
}

func (h *joinRoomHandler) Handle(ctx context.Context, cmd JoinRoom) (JoinRoomResponse, error) {
	sessionKey := "waitroom:" + cmd.TenantID + ":" + cmd.EventID + ":session:" + cmd.SessionID
	arrivalCounterKey := "waitroom:" + cmd.TenantID + ":" + cmd.EventID + ":arrival_counter"
	admittedCounterKey := "waitroom:" + cmd.TenantID + ":" + cmd.EventID + ":admitted_counter"

	exists := true

	position, err := h.rdb.Get(ctx, sessionKey).Int()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			exists = false
		} else {
			return JoinRoomResponse{}, err
		}
	}

	if !exists && position == 0 {
		newPosition, err := h.rdb.Incr(ctx, arrivalCounterKey).Result()
		if err != nil {
			return JoinRoomResponse{}, err
		}
		position = int(newPosition)

		_, err = h.rdb.SetNX(ctx, sessionKey, position, 0).Result()
		if err != nil {
			return JoinRoomResponse{}, err
		}
	}

	admitted, _ := h.rdb.Get(ctx, admittedCounterKey).Int()

	const admissionsPerSecond = 5

	return JoinRoomResponse{
		ArrivalNumber:          position,
		Ahead:                  position - admitted - 1,
		EstimatedWaitInSeconds: int(math.Ceil(float64(position-admitted) / admissionsPerSecond)),
		QueueEnabled:           true,
	}, nil
}
