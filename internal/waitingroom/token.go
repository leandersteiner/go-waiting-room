package waitingroom

type AdmissionToken struct {
	TenantID  string
	EventID   string
	SessionID string
	TokenType string
	Token     string
	ExpiresIn int
}
