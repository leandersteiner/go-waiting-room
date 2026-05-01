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
