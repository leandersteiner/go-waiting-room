package repository

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/leandersteiner/go-waiting-room/internal/waitingroom"
	"github.com/redis/go-redis/v9"
)

const redisKeyPrefix = "waitroom"

var (
	ErrRoomNotFound           = waitingroom.ErrRoomNotFound
	ErrSessionNotFound        = waitingroom.ErrSessionNotFound
	ErrSessionNotAdmitted     = waitingroom.ErrSessionNotAdmitted
	ErrAdmissionCapacityFull  = waitingroom.ErrAdmissionCapacityFull
	ErrAdmissionLeaseMismatch = waitingroom.ErrAdmissionLeaseMismatch
)

type RedisRepository struct {
	rdb            *redis.Client
	tokenIssuer    waitingroom.AdmissionTokenIssuer
	subscriptionMu sync.Mutex
	subscriptions  map[string]*sharedAdmissionProgressSubscription
}

type RoomRef = waitingroom.RoomRef
type AdvanceAdmissionResult = waitingroom.AdmissionAdvanceResult
type AdvanceAdmissionRequest = waitingroom.AdmissionAdvanceRequest

type RedisRepositoryOption func(*RedisRepository)

func WithAdmissionTokenIssuer(issuer waitingroom.AdmissionTokenIssuer) RedisRepositoryOption {
	return func(r *RedisRepository) {
		if issuer != nil {
			r.tokenIssuer = issuer
		}
	}
}

func NewRedisRepository(rdb *redis.Client, options ...RedisRepositoryOption) *RedisRepository {
	if rdb == nil {
		panic("repository: redis client is nil")
	}

	tokenIssuer, err := waitingroom.NewGeneratedJWTAdmissionTokenIssuer("", "", "")
	if err != nil {
		panic(fmt.Sprintf("repository: generate admission token key: %s", err))
	}

	repo := &RedisRepository{
		rdb:           rdb,
		tokenIssuer:   tokenIssuer,
		subscriptions: make(map[string]*sharedAdmissionProgressSubscription),
	}
	for _, option := range options {
		option(repo)
	}

	return repo
}

func (r *RedisRepository) JoinRoom(ctx context.Context, tenantID, eventID, sessionID string) (waitingroom.SessionStatus, error) {
	room, err := r.GetRoom(ctx, tenantID, eventID)
	if err != nil {
		return waitingroom.SessionStatus{}, fmt.Errorf("get waiting room: %w", err)
	}
	if !room.QueueEnabled {
		return waitingroom.SessionStatus{}, ErrRoomNotFound
	}

	position, err := r.joinPosition(ctx, tenantID, eventID, sessionID)
	if err != nil {
		return waitingroom.SessionStatus{}, err
	}

	admitted, err := r.counterValue(ctx, admittedCounterKey(tenantID, eventID))
	if err != nil {
		return waitingroom.SessionStatus{}, fmt.Errorf("read admitted counter: %w", err)
	}

	status := room.SessionStatus(sessionID, position, admitted)
	if err := r.applyAdmissionAvailability(ctx, room, &status); err != nil {
		return waitingroom.SessionStatus{}, err
	}

	return status, nil
}

func (r *RedisRepository) GetSessionStatus(ctx context.Context, tenantID, eventID, sessionID string) (waitingroom.SessionStatus, error) {
	room, err := r.GetRoom(ctx, tenantID, eventID)
	if err != nil {
		return waitingroom.SessionStatus{}, fmt.Errorf("get waiting room: %w", err)
	}
	if !room.QueueEnabled {
		return waitingroom.SessionStatus{}, ErrRoomNotFound
	}

	position, err := r.sessionPosition(ctx, tenantID, eventID, sessionID)
	if err != nil {
		return waitingroom.SessionStatus{}, err
	}

	admitted, err := r.counterValue(ctx, admittedCounterKey(tenantID, eventID))
	if err != nil {
		return waitingroom.SessionStatus{}, fmt.Errorf("read admitted counter: %w", err)
	}

	status := room.SessionStatus(sessionID, position, admitted)
	if err := r.applyAdmissionAvailability(ctx, room, &status); err != nil {
		return waitingroom.SessionStatus{}, err
	}

	return status, nil
}

func (r *RedisRepository) joinPosition(ctx context.Context, tenantID, eventID, sessionID string) (int, error) {
	keys := []string{
		sessionKey(tenantID, eventID, sessionID),
		arrivalCounterKey(tenantID, eventID),
	}

	position, err := joinRoomScript.Run(ctx, r.rdb, keys).Int64()
	if err != nil {
		return 0, fmt.Errorf("join room: %w", err)
	}

	return int64ToInt(position)
}

func (r *RedisRepository) sessionPosition(ctx context.Context, tenantID, eventID, sessionID string) (int, error) {
	position, err := r.rdb.Get(ctx, sessionKey(tenantID, eventID, sessionID)).Int()
	if errors.Is(err, redis.Nil) {
		return 0, ErrSessionNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("read session position: %w", err)
	}

	return position, nil
}

func (r *RedisRepository) counterValue(ctx context.Context, key string) (int, error) {
	value, err := r.rdb.Get(ctx, key).Int()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	return value, nil
}
