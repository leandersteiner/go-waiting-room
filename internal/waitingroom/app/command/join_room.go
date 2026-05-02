package command

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"github.com/leandersteiner/go-waiting-room/internal/waitingroom"
)

const sessionIDBytes = 18

type JoinRoom struct {
	TenantID  string
	EventID   string
	SessionID string
}

type JoinRoomResponse struct {
	SessionID              string
	ArrivalNumber          int
	Position               int
	Ahead                  int
	EstimatedWaitInSeconds int
	CanEnter               bool
	QueueEnabled           bool
}

type JoinRoomHandler func(ctx context.Context, cmd JoinRoom) (JoinRoomResponse, error)

type JoinRoomStore interface {
	JoinRoom(ctx context.Context, tenantID string, eventID string, sessionID string) (waitingroom.SessionStatus, error)
}

type joinRoomHandler struct {
	store JoinRoomStore
}

func NewJoinRoomHandler(store JoinRoomStore) JoinRoomHandler {
	return (&joinRoomHandler{store: store}).Handle
}

func (h *joinRoomHandler) Handle(ctx context.Context, cmd JoinRoom) (JoinRoomResponse, error) {
	if strings.TrimSpace(cmd.SessionID) == "" {
		sessionID, err := newSessionID()
		if err != nil {
			return JoinRoomResponse{}, err
		}
		cmd.SessionID = sessionID
	}

	status, err := h.store.JoinRoom(ctx, cmd.TenantID, cmd.EventID, cmd.SessionID)
	if err != nil {
		if errors.Is(err, waitingroom.ErrRoomNotFound) {
			return JoinRoomResponse{
				QueueEnabled: false,
			}, nil
		}
		return JoinRoomResponse{}, err
	}

	return JoinRoomResponse{
		SessionID:              status.SessionID,
		ArrivalNumber:          status.ArrivalNumber,
		Position:               status.Position,
		Ahead:                  status.Ahead,
		EstimatedWaitInSeconds: status.EstimatedWaitInSeconds,
		CanEnter:               status.CanEnter,
		QueueEnabled:           true,
	}, err
}

func newSessionID() (string, error) {
	value := make([]byte, sessionIDBytes)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("generate session id: %w", err)
	}

	return base64.RawURLEncoding.EncodeToString(value), nil
}
