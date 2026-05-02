package waitingroom

import "testing"

func TestWaitingRoomSessionStatus(t *testing.T) {
	room := WaitingRoom{
		TenantID: "tenant-1",
		EventID:  "event-1",
		AdmissionPolicy: AdmissionPolicy{
			AdmissionsPerSeconds: 50,
		},
	}

	status := room.SessionStatus("session-1", 125, 100)

	if status.TenantID != "tenant-1" {
		t.Fatalf("TenantID = %q, want %q", status.TenantID, "tenant-1")
	}
	if status.EventID != "event-1" {
		t.Fatalf("EventID = %q, want %q", status.EventID, "event-1")
	}
	if status.SessionID != "session-1" {
		t.Fatalf("SessionID = %q, want %q", status.SessionID, "session-1")
	}
	if status.ArrivalNumber != 125 {
		t.Fatalf("ArrivalNumber = %d, want %d", status.ArrivalNumber, 125)
	}
	if status.Position != 25 {
		t.Fatalf("Position = %d, want %d", status.Position, 25)
	}
	if status.Ahead != 24 {
		t.Fatalf("Ahead = %d, want %d", status.Ahead, 24)
	}
	if status.EstimatedWaitInSeconds != 1 {
		t.Fatalf("EstimatedWaitInSeconds = %d, want %d", status.EstimatedWaitInSeconds, 1)
	}
	if status.CanEnter {
		t.Fatal("CanEnter = true, want false")
	}
}

func TestWaitingRoomSessionStatusAdmittedSession(t *testing.T) {
	room := WaitingRoom{
		TenantID: "tenant-1",
		EventID:  "event-1",
		AdmissionPolicy: AdmissionPolicy{
			AdmissionsPerSeconds: 50,
		},
	}

	status := room.SessionStatus("session-1", 10, 10)

	if status.Position != 0 {
		t.Fatalf("Position = %d, want %d", status.Position, 0)
	}
	if status.Ahead != 0 {
		t.Fatalf("Ahead = %d, want %d", status.Ahead, 0)
	}
	if status.EstimatedWaitInSeconds != 0 {
		t.Fatalf("EstimatedWaitInSeconds = %d, want %d", status.EstimatedWaitInSeconds, 0)
	}
	if !status.CanEnter {
		t.Fatal("CanEnter = false, want true")
	}
}

func TestWaitingRoomSessionStatusFrontOfQueueBeforeWorkerAdmission(t *testing.T) {
	room := WaitingRoom{
		TenantID: "tenant-1",
		EventID:  "event-1",
		AdmissionPolicy: AdmissionPolicy{
			AdmissionsPerSeconds: 50,
		},
	}

	status := room.SessionStatus("session-1", 1, 0)

	if status.Position != 1 {
		t.Fatalf("Position = %d, want %d", status.Position, 1)
	}
	if status.Ahead != 0 {
		t.Fatalf("Ahead = %d, want %d", status.Ahead, 0)
	}
	if status.EstimatedWaitInSeconds != 1 {
		t.Fatalf("EstimatedWaitInSeconds = %d, want %d", status.EstimatedWaitInSeconds, 1)
	}
	if status.CanEnter {
		t.Fatal("CanEnter = true, want false")
	}
}

func TestEstimatedWaitInSeconds(t *testing.T) {
	tests := []struct {
		name                string
		remaining           int
		admissionsPerSecond int
		want                int
	}{
		{name: "none remaining", remaining: 0, admissionsPerSecond: 50, want: 0},
		{name: "one partial second", remaining: 1, admissionsPerSecond: 50, want: 1},
		{name: "exact seconds", remaining: 100, admissionsPerSecond: 50, want: 2},
		{name: "rounds up", remaining: 101, admissionsPerSecond: 50, want: 3},
		{name: "invalid rate", remaining: 101, admissionsPerSecond: 0, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := estimatedWaitInSeconds(tt.remaining, tt.admissionsPerSecond)
			if got != tt.want {
				t.Fatalf("estimatedWaitInSeconds(%d, %d) = %d, want %d", tt.remaining, tt.admissionsPerSecond, got, tt.want)
			}
		})
	}
}
