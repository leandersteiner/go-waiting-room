package waitingroom

import "context"

type Repository interface {
	GetRoom(ctx context.Context, tenantID string, eventID string) (WaitingRoom, error)
	GetAdmissionProgress(ctx context.Context, tenantID string, eventID string) (AdmissionProgress, error)
	GetSessionStatus(ctx context.Context, tenantID string, eventID string, sessionID string) (SessionStatus, error)
	JoinRoom(ctx context.Context, tenantID string, eventID string, sessionID string) (SessionStatus, error)
	IssueAdmissionToken(ctx context.Context, tenantID string, eventID string, sessionID string) (AdmissionToken, error)
	ReleaseAdmission(ctx context.Context, tenantID string, eventID string, sessionID string, tokenID string) (bool, error)
}

type AdmissionProgressSubscription interface {
	Updates() <-chan AdmissionProgress
	Close() error
}

type AdmissionProgressSubscriber interface {
	SubscribeAdmissionProgress(ctx context.Context, tenantID string, eventID string) (AdmissionProgressSubscription, error)
}
