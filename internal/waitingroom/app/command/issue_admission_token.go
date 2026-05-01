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
	TokenType string
	Token     string
	ExpiresIn int
}

type IssueAdmissionTokenHandler func(ctx context.Context, cmd IssueAdmissionToken) (IssueAdmissionTokenResponse, error)

type issueAdmissionTokenHandler struct {
	repo waitingroom.Repository
}

func NewIssueAdmissionTokenHandler(repo waitingroom.Repository) IssueAdmissionTokenHandler {
	return (&issueAdmissionTokenHandler{repo: repo}).Handle
}

func (h *issueAdmissionTokenHandler) Handle(ctx context.Context, cmd IssueAdmissionToken) (IssueAdmissionTokenResponse, error) {
	token, err := h.repo.IssueAdmissionToken(ctx, cmd.TenantID, cmd.EventID, cmd.SessionID)
	if err != nil {
		return IssueAdmissionTokenResponse{}, err
	}

	return IssueAdmissionTokenResponse{
		TokenType: token.TokenType,
		Token:     token.Token,
		ExpiresIn: token.ExpiresIn,
	}, nil
}
