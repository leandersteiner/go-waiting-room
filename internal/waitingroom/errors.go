package waitingroom

import "errors"

var (
	ErrRoomNotFound           = errors.New("room not found")
	ErrSessionNotFound        = errors.New("session not found")
	ErrSessionNotAdmitted     = errors.New("session not admitted")
	ErrAdmissionCapacityFull  = errors.New("admission capacity full")
	ErrAdmissionLeaseMismatch = errors.New("admission lease does not match session")
)
