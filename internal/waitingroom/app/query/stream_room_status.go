package query

import (
	"context"
	"time"

	"github.com/leandersteiner/go-waiting-room/internal/waitingroom"
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
	CanEnter               bool `json:"canEnter"`
}

type StreamRoomStatusUpdate func(response StreamRoomStatusResponse) error

type StreamRoomStatusHandler func(ctx context.Context, cmd StreamRoomStatus, update StreamRoomStatusUpdate) error

type streamRoomStatusHandler struct {
	repo waitingroom.Repository
}

func NewStreamRoomStatusHandler(repo waitingroom.Repository) StreamRoomStatusHandler {
	return (&streamRoomStatusHandler{repo: repo}).Handle
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
	status, err := h.repo.GetSessionStatus(ctx, cmd.TenantID, cmd.EventID, cmd.SessionID)
	if err != nil {
		return StreamRoomStatusResponse{}, err
	}

	return StreamRoomStatusResponse{
		ArrivalNumber:          status.ArrivalNumber,
		Position:               status.Position,
		Ahead:                  status.Ahead,
		EstimatedWaitInSeconds: status.EstimatedWaitInSeconds,
		CanEnter:               status.CanEnter,
	}, nil
}
