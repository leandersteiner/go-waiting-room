package waitingroom

type AdmissionPolicy struct {
	AdmissionsPerSeconds       int
	MaxActiveAdmissions        int
	AdmissionOfferTTLInSeconds int
}
type TokenPolicy struct {
	TokenTTLInSeconds int
}

type WaitingRoom struct {
	TenantID        string
	EventID         string
	QueueEnabled    bool
	Version         int
	AdmissionPolicy AdmissionPolicy
	TokenPolicy     TokenPolicy
}
