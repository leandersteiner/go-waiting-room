package command

import (
	"context"

	"github.com/leandersteiner/go-waiting-room/internal/waitingroom"
)

type IssueAdmissionToken struct {
	TenantID  string
	EventID   string
	SessionID string
}

type IssueAdmissionTokenResponse struct {
	TokenID   string
	TokenType string
	Token     string
	ExpiresIn int
}

type IssueAdmissionTokenHandler func(ctx context.Context, cmd IssueAdmissionToken) (IssueAdmissionTokenResponse, error)

type AdmissionTokenStore interface {
	IssueAdmissionToken(ctx context.Context, tenantID string, eventID string, sessionID string) (waitingroom.AdmissionToken, error)
}

type issueAdmissionTokenHandler struct {
	store AdmissionTokenStore
}

func NewIssueAdmissionTokenHandler(store AdmissionTokenStore) IssueAdmissionTokenHandler {
	return (&issueAdmissionTokenHandler{store: store}).Handle
}

func (h *issueAdmissionTokenHandler) Handle(ctx context.Context, cmd IssueAdmissionToken) (IssueAdmissionTokenResponse, error) {
	token, err := h.store.IssueAdmissionToken(ctx, cmd.TenantID, cmd.EventID, cmd.SessionID)
	if err != nil {
		return IssueAdmissionTokenResponse{}, err
	}

	return IssueAdmissionTokenResponse{
		TokenID:   token.TokenID,
		TokenType: token.TokenType,
		Token:     token.Token,
		ExpiresIn: token.ExpiresIn,
	}, nil
}
