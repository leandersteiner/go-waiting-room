package waitingroom

import "context"

type Repository interface {
	GetRoom(ctx context.Context, tenantID string, eventID string) (WaitingRoom, error)
	GetAdmissionProgress(ctx context.Context, tenantID string, eventID string) (AdmissionProgress, error)
	GetSessionStatus(ctx context.Context, tenantID string, eventID string, sessionID string) (SessionStatus, error)
	JoinRoom(ctx context.Context, tenantID string, eventID string, sessionID string) (SessionStatus, error)
	IssueAdmissionToken(ctx context.Context, tenantID string, eventID string, sessionID string) (AdmissionToken, error)
}
