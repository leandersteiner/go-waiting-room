package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"time"

	"github.com/leandersteiner/go-waiting-room/internal/waitingroom"
	"github.com/leandersteiner/go-waiting-room/internal/waitingroom/repository"
)

const (
	defaultTickInterval = time.Second
	defaultLockTTL      = 5 * time.Second
)

type Repository interface {
	TryAcquireWorkerLock(ctx context.Context, owner string, ttl time.Duration) (bool, error)
	ListRooms(ctx context.Context) ([]repository.RoomRef, error)
	GetRoom(ctx context.Context, tenantID string, eventID string) (waitingroom.WaitingRoom, error)
	AdvanceAdmission(ctx context.Context, tenantID string, eventID string, amount int) (repository.AdvanceAdmissionResult, error)
}

type Config struct {
	OwnerID      string
	TickInterval time.Duration
	LockTTL      time.Duration
	Logger       *log.Logger
}

type Worker struct {
	repo         Repository
	ownerID      string
	tickInterval time.Duration
	lockTTL      time.Duration
	logger       *log.Logger
	credits      map[string]float64
}

func New(repo Repository, config Config) (*Worker, error) {
	if repo == nil {
		return nil, errors.New("worker repository is required")
	}
	if config.OwnerID == "" {
		return nil, errors.New("worker owner id is required")
	}
	if config.TickInterval <= 0 {
		config.TickInterval = defaultTickInterval
	}
	if config.LockTTL <= 0 {
		config.LockTTL = defaultLockTTL
	}
	if config.Logger == nil {
		config.Logger = log.New(os.Stdout, "", log.LstdFlags)
	}

	return &Worker{
		repo:         repo,
		ownerID:      config.OwnerID,
		tickInterval: config.TickInterval,
		lockTTL:      config.LockTTL,
		logger:       config.Logger,
		credits:      make(map[string]float64),
	}, nil
}

func (w *Worker) Run(ctx context.Context) error {
	lastTick := time.Now()

	ticker := time.NewTicker(w.tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case now := <-ticker.C:
			elapsed := now.Sub(lastTick)
			lastTick = now
			w.runTick(ctx, elapsed)
		}
	}
}

func (w *Worker) runTick(ctx context.Context, elapsed time.Duration) {
	if err := w.tick(ctx, elapsed); err != nil && ctx.Err() == nil {
		w.logger.Printf("worker tick failed: %s", err)
	}
}

func (w *Worker) tick(ctx context.Context, elapsed time.Duration) error {
	leader, err := w.repo.TryAcquireWorkerLock(ctx, w.ownerID, w.lockTTL)
	if err != nil {
		return err
	}
	if !leader {
		return nil
	}

	rooms, err := w.repo.ListRooms(ctx)
	if err != nil {
		return err
	}

	seen := make(map[string]struct{}, len(rooms))
	for _, roomRef := range rooms {
		key := roomKey(roomRef)
		seen[key] = struct{}{}

		room, err := w.repo.GetRoom(ctx, roomRef.TenantID, roomRef.EventID)
		if errors.Is(err, repository.ErrRoomNotFound) {
			delete(w.credits, key)
			continue
		}
		if err != nil {
			w.logger.Printf("read room tenant=%s event=%s failed: %s", roomRef.TenantID, roomRef.EventID, err)
			continue
		}
		if !room.QueueEnabled || room.AdmissionPolicy.AdmissionsPerSeconds <= 0 {
			delete(w.credits, key)
			continue
		}

		amount := w.admissionAmount(key, room.AdmissionPolicy.AdmissionsPerSeconds, elapsed)
		if amount <= 0 {
			continue
		}

		result, err := w.repo.AdvanceAdmission(ctx, roomRef.TenantID, roomRef.EventID, amount)
		if err != nil {
			w.logger.Printf("advance room tenant=%s event=%s failed: %s", roomRef.TenantID, roomRef.EventID, err)
			continue
		}
		if result.Advanced > 0 {
			w.logger.Printf(
				"advanced room tenant=%s event=%s amount=%d admitted=%d arrived=%d",
				roomRef.TenantID,
				roomRef.EventID,
				result.Advanced,
				result.Progress.AdmittedCounter,
				result.Progress.ArrivalCounter,
			)
		}
	}

	for key := range w.credits {
		if _, ok := seen[key]; !ok {
			delete(w.credits, key)
		}
	}

	return nil
}

func (w *Worker) admissionAmount(key string, admissionsPerSecond int, elapsed time.Duration) int {
	credits := w.credits[key] + float64(admissionsPerSecond)*elapsed.Seconds()
	if credits < 1 {
		w.credits[key] = credits
		return 0
	}

	amount := int(math.Floor(credits))
	w.credits[key] = credits - float64(amount)
	return amount
}

func roomKey(room repository.RoomRef) string {
	return fmt.Sprintf("%s:%s", room.TenantID, room.EventID)
}
