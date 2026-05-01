package waitingroom

import "time"

type Status string

const (
	Scheduled Status = "scheduled"
	Warmup    Status = "warmup"
	Active    Status = "active"
	Paused    Status = "paused"
	Draining  Status = "draining"
	Completed Status = "completed"
)

type AdmissionPolicy string
type TokenPolicy string

type WaitingRoom struct {
	tenantID        string
	eventID         string
	status          Status
	admissionPolicy AdmissionPolicy
	tokenPolicy     TokenPolicy
	configVersion   int
	startsAt        time.Time
	endsAt          time.Time
}
