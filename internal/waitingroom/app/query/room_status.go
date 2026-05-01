package query

import (
	"context"
	"errors"
	"math"

	"github.com/redis/go-redis/v9"
)

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

type roomStatusHandler struct {
	rdb *redis.Client
}

func NewRoomStatusHandler(rdb *redis.Client) RoomStatusHandler {
	return (&roomStatusHandler{rdb: rdb}).Handle
}

func (h *roomStatusHandler) Handle(ctx context.Context, query RoomStatus) (RoomStatusResponse, error) {
	sessionKey := "waitroom:" + query.TenantID + ":" + query.EventID + ":session:" + query.SessionID
	admittedCounterKey := "waitroom:" + query.TenantID + ":" + query.EventID + ":admitted_counter"

	position, err := h.rdb.Get(ctx, sessionKey).Int()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return RoomStatusResponse{}, errors.New("session not found")
		}

		return RoomStatusResponse{}, err
	}

	admitted, _ := h.rdb.Get(ctx, admittedCounterKey).Int()

	const admissionsPerSecond = 5

	return RoomStatusResponse{
		Ahead:                  position - admitted - 1,
		EstimatedWaitInSeconds: int(math.Ceil(float64(position-admitted) / admissionsPerSecond)),
	}, nil
}
