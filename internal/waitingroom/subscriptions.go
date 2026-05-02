package waitingroom

import "context"

type AdmissionProgressSubscription interface {
	Updates() <-chan AdmissionProgress
	Close() error
}

type AdmissionProgressSubscriber interface {
	SubscribeAdmissionProgress(ctx context.Context, tenantID string, eventID string) (AdmissionProgressSubscription, error)
}
