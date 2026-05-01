package query

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/redis/go-redis/v9"
)

type StreamRoomStatus struct {
	TenantID       string
	EventID        string
	SessionID      string
	UpdateInterval time.Duration
}

type StreamRoomStatusResponse struct {
	ArrivalNumber          int  `json:"arrivalNumber"`
	Position               int  `json:"position"`
	Ahead                  int  `json:"ahead"`
	EstimatedWaitInSeconds int  `json:"estimatedWaitInSeconds"`
	QueueEnabled           bool `json:"queueEnabled"`
	CanEnter               bool `json:"canEnter"`
}

type StreamRoomStatusUpdate func(response StreamRoomStatusResponse) error

type StreamRoomStatusHandler func(ctx context.Context, cmd StreamRoomStatus, update StreamRoomStatusUpdate) error

type streamRoomStatusHandler struct {
	rdb *redis.Client
}

func NewStreamRoomStatusHandler(rdb *redis.Client) StreamRoomStatusHandler {
	return (&streamRoomStatusHandler{rdb: rdb}).Handle
}

func (h *streamRoomStatusHandler) Handle(ctx context.Context, cmd StreamRoomStatus, update StreamRoomStatusUpdate) error {
	if cmd.UpdateInterval <= 0 {
		cmd.UpdateInterval = time.Second
	}

	if err := h.sendUpdate(ctx, cmd, update); err != nil {
		return err
	}

	ticker := time.NewTicker(cmd.UpdateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := h.sendUpdate(ctx, cmd, update); err != nil {
				return err
			}
		}
	}
}

func (h *streamRoomStatusHandler) sendUpdate(ctx context.Context, cmd StreamRoomStatus, update StreamRoomStatusUpdate) error {
	response, err := h.roomStatus(ctx, cmd)
	if err != nil {
		return err
	}

	return update(response)
}

func (h *streamRoomStatusHandler) roomStatus(ctx context.Context, cmd StreamRoomStatus) (StreamRoomStatusResponse, error) {
	sessionKey := "waitroom:" + cmd.TenantID + ":" + cmd.EventID + ":session:" + cmd.SessionID
	admittedCounterKey := "waitroom:" + cmd.TenantID + ":" + cmd.EventID + ":admitted_counter"

	position, err := h.rdb.Get(ctx, sessionKey).Int()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return StreamRoomStatusResponse{}, errors.New("session not found")
		}

		return StreamRoomStatusResponse{}, err
	}

	admitted, _ := h.rdb.Get(ctx, admittedCounterKey).Int()

	const admissionsPerSecond = 5

	ahead := position - admitted - 1
	if ahead < 0 {
		ahead = 0
	}

	remaining := position - admitted
	if remaining < 0 {
		remaining = 0
	}

	return StreamRoomStatusResponse{
		ArrivalNumber:          position,
		Position:               ahead + 1,
		Ahead:                  ahead,
		EstimatedWaitInSeconds: int(math.Ceil(float64(remaining) / admissionsPerSecond)),
		QueueEnabled:           true,
		CanEnter:               ahead == 0,
	}, nil
}
