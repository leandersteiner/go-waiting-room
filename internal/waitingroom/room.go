package waitingroom

const (
	DefaultAdmissionsPerSecond        = 250
	DefaultTokenTTLInSeconds          = 900
	DefaultMaxActiveAdmissions        = 250
	DefaultAdmissionOfferTTLInSeconds = 60
	DefaultQueueEnabled               = true
)

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

func NewDefaultRoom(tenantID, eventID string) WaitingRoom {
	return WaitingRoom{
		TenantID:     tenantID,
		EventID:      eventID,
		QueueEnabled: DefaultQueueEnabled,
		AdmissionPolicy: AdmissionPolicy{
			AdmissionsPerSeconds:       DefaultAdmissionsPerSecond,
			MaxActiveAdmissions:        DefaultMaxActiveAdmissions,
			AdmissionOfferTTLInSeconds: DefaultAdmissionOfferTTLInSeconds,
		},
		TokenPolicy: TokenPolicy{
			TokenTTLInSeconds: DefaultTokenTTLInSeconds,
		},
	}
}

func (r WaitingRoom) SessionStatus(sessionID string, arrivalNumber, admittedCounter int) SessionStatus {
	remaining := nonNegative(arrivalNumber - admittedCounter)
	ahead := nonNegative(remaining - 1)

	return SessionStatus{
		TenantID:               r.TenantID,
		EventID:                r.EventID,
		SessionID:              sessionID,
		ArrivalNumber:          arrivalNumber,
		Position:               remaining,
		Ahead:                  ahead,
		EstimatedWaitInSeconds: estimatedWaitInSeconds(remaining, r.AdmissionPolicy.AdmissionsPerSeconds),
		CanEnter:               arrivalNumber <= admittedCounter,
	}
}

func (r WaitingRoom) CanClaimAdmission(hasValidOffer bool, activeAdmissions, admissionOffers int) bool {
	return r.AdmissionPolicy.CanClaimAdmission(hasValidOffer, activeAdmissions, admissionOffers)
}

func (r WaitingRoom) NewAdmissionAdvanceRequest(amount int) AdmissionAdvanceRequest {
	return AdmissionAdvanceRequest{
		Amount:                     amount,
		MaxActiveAdmissions:        r.AdmissionPolicy.MaxActiveAdmissions,
		AdmissionOfferTTLInSeconds: r.AdmissionPolicy.AdmissionOfferTTLInSeconds,
	}
}

func (p AdmissionPolicy) CanClaimAdmission(hasValidOffer bool, activeAdmissions, admissionOffers int) bool {
	if p.MaxActiveAdmissions <= 0 {
		return false
	}
	if hasValidOffer {
		return true
	}

	return p.AvailableAdmissionSlots(activeAdmissions, admissionOffers) > 0
}

func (p AdmissionPolicy) AvailableAdmissionSlots(activeAdmissions, admissionOffers int) int {
	available := p.MaxActiveAdmissions - activeAdmissions - admissionOffers
	return nonNegative(available)
}
