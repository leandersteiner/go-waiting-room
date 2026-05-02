package worker

import (
	"context"
	"io"
	"log"
	"testing"
	"time"

	"github.com/leandersteiner/go-waiting-room/internal/waitingroom"
	"github.com/leandersteiner/go-waiting-room/internal/waitingroom/repository"
)

func TestTickRequiresLeadership(t *testing.T) {
	repo := &fakeRepository{leader: false}
	worker := newTestWorker(t, repo)

	if err := worker.tick(context.Background(), time.Second); err != nil {
		t.Fatal(err)
	}

	if repo.listCalls != 0 {
		t.Fatalf("ListRooms calls = %d, want 0", repo.listCalls)
	}
}

func TestTickAccumulatesFractionalAdmissionCredits(t *testing.T) {
	repo := &fakeRepository{
		leader: true,
		rooms:  []repository.RoomRef{{TenantID: "tenant-1", EventID: "event-1"}},
		room: waitingroom.WaitingRoom{
			TenantID:     "tenant-1",
			EventID:      "event-1",
			QueueEnabled: true,
			AdmissionPolicy: waitingroom.AdmissionPolicy{
				AdmissionsPerSeconds: 5,
			},
		},
	}
	worker := newTestWorker(t, repo)

	if err := worker.tick(context.Background(), 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if len(repo.advanceAmounts) != 0 {
		t.Fatalf("advance amounts = %v, want none", repo.advanceAmounts)
	}

	if err := worker.tick(context.Background(), 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if len(repo.advanceAmounts) != 1 || repo.advanceAmounts[0] != 1 {
		t.Fatalf("advance amounts = %v, want [1]", repo.advanceAmounts)
	}
}

func newTestWorker(t *testing.T, repo Repository) *Worker {
	t.Helper()

	worker, err := New(repo, Config{
		OwnerID: "worker-1",
		Logger:  log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}

	return worker
}

type fakeRepository struct {
	leader         bool
	rooms          []repository.RoomRef
	room           waitingroom.WaitingRoom
	listCalls      int
	advanceAmounts []int
}

func (r *fakeRepository) TryAcquireWorkerLock(context.Context, string, time.Duration) (bool, error) {
	return r.leader, nil
}

func (r *fakeRepository) ListRooms(context.Context) ([]repository.RoomRef, error) {
	r.listCalls++
	return r.rooms, nil
}

func (r *fakeRepository) GetRoom(context.Context, string, string) (waitingroom.WaitingRoom, error) {
	return r.room, nil
}

func (r *fakeRepository) AdvanceAdmission(_ context.Context, tenantID string, eventID string, amount int) (repository.AdvanceAdmissionResult, error) {
	r.advanceAmounts = append(r.advanceAmounts, amount)
	return repository.AdvanceAdmissionResult{
		Progress: waitingroom.AdmissionProgress{
			TenantID:        tenantID,
			EventID:         eventID,
			ArrivalCounter:  amount,
			AdmittedCounter: amount,
		},
		Advanced: amount,
	}, nil
}
