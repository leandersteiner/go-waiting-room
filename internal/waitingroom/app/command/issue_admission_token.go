package command

import "context"

type IssueAdmissionToken struct {
	SessionID string
}

type IssueAdmissionTokenResponse struct {
	TokenType   string
	AccessToken string
	ExpiresIn   int
}

type IssueAdmissionTokenHandler func(ctx context.Context, cmd IssueAdmissionToken) (IssueAdmissionTokenResponse, error)

type issueAdmissionTokenHandler struct{}

func NewIssueAdmissionTokenHandler() IssueAdmissionTokenHandler {
	return (&issueAdmissionTokenHandler{}).Handle
}

func (h *issueAdmissionTokenHandler) Handle(ctx context.Context, cmd IssueAdmissionToken) (IssueAdmissionTokenResponse, error) {
	return IssueAdmissionTokenResponse{}, nil
}
