package waitingroom

import "time"

type AdmissionProgress struct {
	tenantID        string
	eventID         string
	arrivalCounter  int
	admittedCounter int
	lastAdmissionAt time.Time
}
