package waitingroom

type AdmissionProgress struct {
	TenantID            string
	EventID             string
	ArrivalCounter      int
	AdmittedCounter     int
	ActiveAdmissions    int
	AdmissionOffers     int
	MaxActiveAdmissions int
}

type RoomRef struct {
	TenantID string
	EventID  string
}

type AdmissionAdvanceRequest struct {
	Amount                     int
	MaxActiveAdmissions        int
	AdmissionOfferTTLInSeconds int
}

type AdmissionAdvanceResult struct {
	Progress AdmissionProgress
	Advanced int
}

func NewAdmissionProgress(room WaitingRoom, arrivalCounter, admittedCounter, activeAdmissions, admissionOffers int) AdmissionProgress {
	return AdmissionProgress{
		TenantID:            room.TenantID,
		EventID:             room.EventID,
		ArrivalCounter:      arrivalCounter,
		AdmittedCounter:     admittedCounter,
		ActiveAdmissions:    activeAdmissions,
		AdmissionOffers:     admissionOffers,
		MaxActiveAdmissions: room.AdmissionPolicy.MaxActiveAdmissions,
	}
}

func (p AdmissionProgress) AdmissionAdvanceResult(previousAdmittedCounter int) AdmissionAdvanceResult {
	advanced := p.AdmittedCounter - previousAdmittedCounter
	if advanced < 0 {
		advanced = 0
	}

	return AdmissionAdvanceResult{
		Progress: p,
		Advanced: advanced,
	}
}
