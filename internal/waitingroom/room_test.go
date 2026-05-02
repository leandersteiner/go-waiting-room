package waitingroom

import "testing"

func TestAdmissionPolicyCanClaimAdmission(t *testing.T) {
	tests := []struct {
		name         string
		policy       AdmissionPolicy
		validOffer   bool
		active       int
		offers       int
		wantCanClaim bool
	}{
		{
			name:         "valid offer can claim",
			policy:       AdmissionPolicy{MaxActiveAdmissions: 10},
			validOffer:   true,
			active:       10,
			offers:       0,
			wantCanClaim: true,
		},
		{
			name:         "free capacity can claim",
			policy:       AdmissionPolicy{MaxActiveAdmissions: 10},
			active:       8,
			offers:       1,
			wantCanClaim: true,
		},
		{
			name:         "full capacity cannot claim",
			policy:       AdmissionPolicy{MaxActiveAdmissions: 10},
			active:       9,
			offers:       1,
			wantCanClaim: false,
		},
		{
			name:         "disabled capacity cannot claim",
			policy:       AdmissionPolicy{MaxActiveAdmissions: 0},
			validOffer:   true,
			wantCanClaim: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.policy.CanClaimAdmission(tt.validOffer, tt.active, tt.offers)
			if got != tt.wantCanClaim {
				t.Fatalf("CanClaimAdmission() = %t, want %t", got, tt.wantCanClaim)
			}
		})
	}
}
