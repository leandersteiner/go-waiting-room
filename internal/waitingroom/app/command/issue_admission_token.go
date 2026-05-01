package command

import (
	"context"
	"errors"

	"github.com/redis/go-redis/v9"
)

type IssueAdmissionToken struct {
	TenantID  string
	EventID   string
	SessionID string
}

type IssueAdmissionTokenResponse struct {
	TokenType   string
	AccessToken string
	ExpiresIn   int
}

type IssueAdmissionTokenHandler func(ctx context.Context, cmd IssueAdmissionToken) (IssueAdmissionTokenResponse, error)

type issueAdmissionTokenHandler struct {
	rdb *redis.Client
}

func NewIssueAdmissionTokenHandler(rdb *redis.Client) IssueAdmissionTokenHandler {
	return (&issueAdmissionTokenHandler{rdb: rdb}).Handle
}

func (h *issueAdmissionTokenHandler) Handle(ctx context.Context, cmd IssueAdmissionToken) (IssueAdmissionTokenResponse, error) {
	sessionKey := "waitroom:" + cmd.TenantID + ":" + cmd.EventID + ":session:" + cmd.SessionID
	admittedCounterKey := "waitroom:" + cmd.TenantID + ":" + cmd.EventID + ":admitted_counter"

	position, err := h.rdb.Get(ctx, sessionKey).Int()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return IssueAdmissionTokenResponse{}, errors.New("session not found")
		}

		return IssueAdmissionTokenResponse{}, err
	}

	admitted, _ := h.rdb.Get(ctx, admittedCounterKey).Int()

	if position > admitted {
		return IssueAdmissionTokenResponse{}, errors.New("session not admitted")
	}

	return IssueAdmissionTokenResponse{
		TokenType:   "Bearer",
		AccessToken: "accessToken",
		ExpiresIn:   300,
	}, nil
}
