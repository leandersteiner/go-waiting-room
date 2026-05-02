package command

import (
	"context"
	"errors"
	"strings"
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

type AdmissionReleaseStore interface {
	ReleaseAdmission(ctx context.Context, tenantID string, eventID string, sessionID string, tokenID string) (bool, error)
}

type releaseAdmissionHandler struct {
	store AdmissionReleaseStore
}

func NewReleaseAdmissionHandler(store AdmissionReleaseStore) ReleaseAdmissionHandler {
	return (&releaseAdmissionHandler{store: store}).Handle
}

func (h *releaseAdmissionHandler) Handle(ctx context.Context, cmd ReleaseAdmission) (ReleaseAdmissionResponse, error) {
	if strings.TrimSpace(cmd.SessionID) == "" || strings.TrimSpace(cmd.TokenID) == "" {
		return ReleaseAdmissionResponse{}, ErrInvalidReleaseAdmission
	}

	released, err := h.store.ReleaseAdmission(ctx, cmd.TenantID, cmd.EventID, cmd.SessionID, cmd.TokenID)
	if err != nil {
		return ReleaseAdmissionResponse{}, err
	}

	return ReleaseAdmissionResponse{Released: released}, nil
}
