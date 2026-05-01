package repository

import (
	"context"
	"errors"
	"math"

	"github.com/leandersteiner/go-waiting-room/internal/waitingroom"
	"github.com/redis/go-redis/v9"
)

var ErrRoomNotFound = errors.New("room not found")

var _ waitingroom.Repository = (*RedisRepository)(nil)

var arrivalCounterKey = func(tenantID string, eventID string) string {
	return "waitroom:" + tenantID + ":" + eventID + ":arrival_counter"
}

var admittedCounterKey = func(tenantID string, eventID string) string {
	return "waitroom:" + tenantID + ":" + eventID + ":admitted_counter"
}

var sessionKey = func(tenantID string, eventID string, sessionID string) string {
	return "waitroom:" + tenantID + ":" + eventID + ":session:" + sessionID
}

type RedisRepository struct {
	rdb *redis.Client
}

func NewRedisRepository(rdb *redis.Client) *RedisRepository {
	return &RedisRepository{rdb: rdb}
}

func (r *RedisRepository) IssueAdmissionToken(ctx context.Context, tenantID string, eventID string, sessionID string) (waitingroom.AdmissionToken, error) {
	room, err := r.GetRoom(ctx, tenantID, eventID)
	if err != nil {
		return waitingroom.AdmissionToken{}, ErrRoomNotFound
	}

	position, err := r.rdb.Get(ctx, sessionKey(tenantID, eventID, sessionID)).Int()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return waitingroom.AdmissionToken{}, errors.New("session not found")
		}

		return waitingroom.AdmissionToken{}, err
	}

	admitted, _ := r.rdb.Get(ctx, admittedCounterKey(tenantID, eventID)).Int()

	if position-1 > admitted {
		return waitingroom.AdmissionToken{}, errors.New("session not admitted")
	}

	err = r.rdb.Incr(ctx, admittedCounterKey(tenantID, eventID)).Err()
	if err != nil {
		return waitingroom.AdmissionToken{}, err
	}

	err = r.rdb.Del(ctx, sessionKey(tenantID, eventID, sessionID)).Err()
	if err != nil {
		return waitingroom.AdmissionToken{}, err
	}

	return waitingroom.AdmissionToken{
		TokenType: "Bearer",
		Token:     "accessToken",
		ExpiresIn: room.TokenPolicy.TokenTTLInSeconds,
	}, nil
}

func (r *RedisRepository) JoinRoom(ctx context.Context, tenantID string, eventID string, sessionID string) (waitingroom.SessionStatus, error) {
	room, err := r.GetRoom(ctx, tenantID, eventID)
	if err != nil {
		return waitingroom.SessionStatus{}, ErrRoomNotFound
	}

	exists := true

	position, err := r.rdb.Get(ctx, sessionKey(tenantID, eventID, sessionID)).Int()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			exists = false
		} else {
			return waitingroom.SessionStatus{}, err
		}
	}

	if !exists && position == 0 {
		newPosition, err := r.rdb.Incr(ctx, arrivalCounterKey(tenantID, eventID)).Result()
		if err != nil {
			return waitingroom.SessionStatus{}, err
		}
		position = int(newPosition)

		_, err = r.rdb.SetNX(ctx, sessionKey(tenantID, eventID, sessionID), position, 0).Result()
		if err != nil {
			return waitingroom.SessionStatus{}, err
		}
	}

	admitted, _ := r.rdb.Get(ctx, admittedCounterKey(tenantID, eventID)).Int()

	ahead := position - admitted - 1
	if ahead < 0 {
		ahead = 0
	}

	remaining := position - admitted
	if remaining < 0 {
		remaining = 0
	}

	return waitingroom.SessionStatus{
		TenantID:               tenantID,
		EventID:                eventID,
		SessionID:              sessionID,
		ArrivalNumber:          position,
		Position:               ahead + 1,
		Ahead:                  ahead,
		EstimatedWaitInSeconds: int(math.Ceil(float64(remaining) / float64(room.AdmissionPolicy.AdmissionsPerSeconds))),
		CanEnter:               ahead == 0,
	}, nil
}

func (r *RedisRepository) GetRoom(ctx context.Context, tenantID string, eventID string) (waitingroom.WaitingRoom, error) {
	return waitingroom.WaitingRoom{
		TenantID: tenantID,
		EventID:  eventID,
		AdmissionPolicy: waitingroom.AdmissionPolicy{
			AdmissionsPerSeconds: 50,
		},
		TokenPolicy: waitingroom.TokenPolicy{
			TokenTTLInSeconds: 300,
		},
	}, nil
}

func (r *RedisRepository) GetAdmissionProgress(ctx context.Context, tenantID string, eventID string) (waitingroom.AdmissionProgress, error) {
	admitted, err := r.rdb.Get(ctx, admittedCounterKey(tenantID, eventID)).Int()
	if err != nil && !errors.Is(err, redis.Nil) {
		return waitingroom.AdmissionProgress{}, err
	}

	arrived, err := r.rdb.Get(ctx, arrivalCounterKey(tenantID, eventID)).Int()
	if err != nil && !errors.Is(err, redis.Nil) {
		return waitingroom.AdmissionProgress{}, err
	}

	return waitingroom.AdmissionProgress{
		TenantID:        tenantID,
		EventID:         eventID,
		ArrivalCounter:  arrived,
		AdmittedCounter: admitted,
	}, nil
}

func (r *RedisRepository) GetSessionStatus(ctx context.Context, tenantID string, eventID string, sessionID string) (waitingroom.SessionStatus, error) {
	room, err := r.GetRoom(ctx, tenantID, eventID)
	if err != nil {
		return waitingroom.SessionStatus{}, err
	}

	position, err := r.rdb.Get(ctx, sessionKey(tenantID, eventID, sessionID)).Int()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return waitingroom.SessionStatus{}, errors.New("session not found")
		}

		return waitingroom.SessionStatus{}, err
	}

	admitted, _ := r.rdb.Get(ctx, admittedCounterKey(tenantID, eventID)).Int()

	ahead := position - admitted - 1
	if ahead < 0 {
		ahead = 0
	}

	remaining := position - admitted
	if remaining < 0 {
		remaining = 0
	}

	return waitingroom.SessionStatus{
		TenantID:               tenantID,
		EventID:                eventID,
		SessionID:              sessionID,
		ArrivalNumber:          position,
		Position:               ahead + 1,
		Ahead:                  ahead,
		EstimatedWaitInSeconds: int(math.Ceil(float64(remaining) / float64(room.AdmissionPolicy.AdmissionsPerSeconds))),
		CanEnter:               ahead == 0,
	}, nil
}
