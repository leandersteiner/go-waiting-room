package waitingroom

type AdmissionPolicy struct {
	AdmissionsPerSeconds int
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
