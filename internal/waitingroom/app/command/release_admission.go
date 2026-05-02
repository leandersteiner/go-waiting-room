package command

import (
	"context"
	"errors"
	"strings"

	"github.com/leandersteiner/go-waiting-room/internal/waitingroom"
)

var ErrInvalidReleaseAdmission = errors.New("sessionID and tokenID are required")

type ReleaseAdmission struct {
	TenantID  string
	EventID   string
	SessionID string
	TokenID   string
}

type ReleaseAdmissionResponse struct {
	Released bool
}

type ReleaseAdmissionHandler func(ctx context.Context, cmd ReleaseAdmission) (ReleaseAdmissionResponse, error)

type releaseAdmissionHandler struct {
	repo waitingroom.Repository
}

func NewReleaseAdmissionHandler(repo waitingroom.Repository) ReleaseAdmissionHandler {
	return (&releaseAdmissionHandler{repo: repo}).Handle
}

func (h *releaseAdmissionHandler) Handle(ctx context.Context, cmd ReleaseAdmission) (ReleaseAdmissionResponse, error) {
	if strings.TrimSpace(cmd.SessionID) == "" || strings.TrimSpace(cmd.TokenID) == "" {
		return ReleaseAdmissionResponse{}, ErrInvalidReleaseAdmission
	}

	released, err := h.repo.ReleaseAdmission(ctx, cmd.TenantID, cmd.EventID, cmd.SessionID, cmd.TokenID)
	if err != nil {
		return ReleaseAdmissionResponse{}, err
	}

	return ReleaseAdmissionResponse{Released: released}, nil
}
