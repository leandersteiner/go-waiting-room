package waitingroom

type SessionStatus struct {
	TenantID               string
	EventID                string
	SessionID              string
	ArrivalNumber          int
	Position               int
	Ahead                  int
	EstimatedWaitInSeconds int
	CanEnter               bool
}

func (s SessionStatus) WithAdmissionAvailability(canClaimAdmission bool) SessionStatus {
	if s.CanEnter {
		s.CanEnter = canClaimAdmission
	}

	return s
}

func estimatedWaitInSeconds(remaining, admissionsPerSecond int) int {
	if remaining <= 0 || admissionsPerSecond <= 0 {
		return 0
	}

	seconds := remaining / admissionsPerSecond
	if remaining%admissionsPerSecond != 0 {
		seconds++
	}

	return seconds
}

func nonNegative(value int) int {
	if value < 0 {
		return 0
	}

	return value
}
