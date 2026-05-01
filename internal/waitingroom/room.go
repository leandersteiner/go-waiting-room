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
	AdmissionPolicy AdmissionPolicy
	TokenPolicy     TokenPolicy
}
