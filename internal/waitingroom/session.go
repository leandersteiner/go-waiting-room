package waitingroom

import "time"

type State string

const (
	Waiting  State = "waiting"
	Admitted State = "admitted"
	Expired  State = "expired"
)

type QueueSession struct {
	tenantID      string
	eventID       string
	sessionID     string
	arrivalNumber int
	state         State
	joinedAt      time.Time
	admittedAt    time.Time
}
