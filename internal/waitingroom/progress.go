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
