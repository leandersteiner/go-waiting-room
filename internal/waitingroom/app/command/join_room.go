package command

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"github.com/leandersteiner/go-waiting-room/internal/waitingroom"
	"github.com/leandersteiner/go-waiting-room/internal/waitingroom/repository"
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

type joinRoomHandler struct {
	repo waitingroom.Repository
}

func NewJoinRoomHandler(repo waitingroom.Repository) JoinRoomHandler {
	return (&joinRoomHandler{repo: repo}).Handle
}

func (h *joinRoomHandler) Handle(ctx context.Context, cmd JoinRoom) (JoinRoomResponse, error) {
	if strings.TrimSpace(cmd.SessionID) == "" {
		sessionID, err := newSessionID()
		if err != nil {
			return JoinRoomResponse{}, err
		}
		cmd.SessionID = sessionID
	}

	status, err := h.repo.JoinRoom(ctx, cmd.TenantID, cmd.EventID, cmd.SessionID)
	if err != nil {
		if errors.Is(err, repository.ErrRoomNotFound) {
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
