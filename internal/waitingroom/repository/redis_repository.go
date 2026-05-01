package repository

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/leandersteiner/go-waiting-room/internal/waitingroom"
	"github.com/redis/go-redis/v9"
)

const (
	redisKeyPrefix = "waitroom"

	defaultAdmissionsPerSecond = 50
	defaultTokenTTLInSeconds   = 300

	admissionTokenBytes = 32
	bearerTokenType     = "Bearer"
)

var (
	ErrRoomNotFound       = errors.New("room not found")
	ErrSessionNotFound    = errors.New("session not found")
	ErrSessionNotAdmitted = errors.New("session not admitted")
)

var _ waitingroom.Repository = (*RedisRepository)(nil)

var joinRoomScript = redis.NewScript(`
local position = redis.call("GET", KEYS[1])
if position then
	return position
end

position = redis.call("INCR", KEYS[2])
redis.call("SET", KEYS[1], position)
return position
`)

var issueAdmissionTokenScript = redis.NewScript(`
local position = redis.call("GET", KEYS[1])
if not position then
	return {0, 0}
end

position = tonumber(position)
if not position then
	return redis.error_reply("invalid session position")
end

local admitted = tonumber(redis.call("GET", KEYS[2]) or "0")
if not admitted then
	return redis.error_reply("invalid admitted counter")
end

if position - 1 > admitted then
	return {1, admitted}
end

local updatedAdmitted = redis.call("INCR", KEYS[2])
redis.call("DEL", KEYS[1])
return {2, updatedAdmitted}
`)

type tokenGenerator func() (string, error)

type RedisRepository struct {
	rdb      *redis.Client
	newToken tokenGenerator
}

func NewRedisRepository(rdb *redis.Client) *RedisRepository {
	if rdb == nil {
		panic("repository: redis client is nil")
	}

	return &RedisRepository{
		rdb:      rdb,
		newToken: generateAdmissionToken,
	}
}

func (r *RedisRepository) IssueAdmissionToken(ctx context.Context, tenantID, eventID, sessionID string) (waitingroom.AdmissionToken, error) {
	room, err := r.GetRoom(ctx, tenantID, eventID)
	if err != nil {
		return waitingroom.AdmissionToken{}, fmt.Errorf("get waiting room: %w", err)
	}

	token, err := r.newToken()
	if err != nil {
		return waitingroom.AdmissionToken{}, fmt.Errorf("generate admission token: %w", err)
	}

	keys := []string{
		sessionKey(tenantID, eventID, sessionID),
		admittedCounterKey(tenantID, eventID),
	}
	result, err := issueAdmissionTokenScript.Run(ctx, r.rdb, keys).Int64Slice()
	if err != nil {
		return waitingroom.AdmissionToken{}, fmt.Errorf("issue admission token: %w", err)
	}
	if len(result) != 2 {
		return waitingroom.AdmissionToken{}, fmt.Errorf("issue admission token: unexpected redis result length %d", len(result))
	}

	switch result[0] {
	case 0:
		return waitingroom.AdmissionToken{}, ErrSessionNotFound
	case 1:
		return waitingroom.AdmissionToken{}, ErrSessionNotAdmitted
	case 2:
		return waitingroom.AdmissionToken{
			TenantID:  tenantID,
			EventID:   eventID,
			SessionID: sessionID,
			TokenType: bearerTokenType,
			Token:     token,
			ExpiresIn: room.TokenPolicy.TokenTTLInSeconds,
		}, nil
	default:
		return waitingroom.AdmissionToken{}, fmt.Errorf("issue admission token: unexpected redis status %d", result[0])
	}
}

func (r *RedisRepository) JoinRoom(ctx context.Context, tenantID, eventID, sessionID string) (waitingroom.SessionStatus, error) {
	room, err := r.GetRoom(ctx, tenantID, eventID)
	if err != nil {
		return waitingroom.SessionStatus{}, fmt.Errorf("get waiting room: %w", err)
	}

	position, err := r.joinPosition(ctx, tenantID, eventID, sessionID)
	if err != nil {
		return waitingroom.SessionStatus{}, err
	}

	admitted, err := r.counterValue(ctx, admittedCounterKey(tenantID, eventID))
	if err != nil {
		return waitingroom.SessionStatus{}, fmt.Errorf("read admitted counter: %w", err)
	}

	return newSessionStatus(room, sessionID, position, admitted), nil
}

func (*RedisRepository) GetRoom(ctx context.Context, tenantID, eventID string) (waitingroom.WaitingRoom, error) {
	return waitingroom.WaitingRoom{
		TenantID: tenantID,
		EventID:  eventID,
		AdmissionPolicy: waitingroom.AdmissionPolicy{
			AdmissionsPerSeconds: defaultAdmissionsPerSecond,
		},
		TokenPolicy: waitingroom.TokenPolicy{
			TokenTTLInSeconds: defaultTokenTTLInSeconds,
		},
	}, nil
}

func (r *RedisRepository) GetAdmissionProgress(ctx context.Context, tenantID, eventID string) (waitingroom.AdmissionProgress, error) {
	arrived, err := r.counterValue(ctx, arrivalCounterKey(tenantID, eventID))
	if err != nil {
		return waitingroom.AdmissionProgress{}, fmt.Errorf("read arrival counter: %w", err)
	}

	admitted, err := r.counterValue(ctx, admittedCounterKey(tenantID, eventID))
	if err != nil {
		return waitingroom.AdmissionProgress{}, fmt.Errorf("read admitted counter: %w", err)
	}

	return waitingroom.AdmissionProgress{
		TenantID:        tenantID,
		EventID:         eventID,
		ArrivalCounter:  arrived,
		AdmittedCounter: admitted,
	}, nil
}

func (r *RedisRepository) GetSessionStatus(ctx context.Context, tenantID, eventID, sessionID string) (waitingroom.SessionStatus, error) {
	room, err := r.GetRoom(ctx, tenantID, eventID)
	if err != nil {
		return waitingroom.SessionStatus{}, fmt.Errorf("get waiting room: %w", err)
	}

	position, err := r.sessionPosition(ctx, tenantID, eventID, sessionID)
	if err != nil {
		return waitingroom.SessionStatus{}, err
	}

	admitted, err := r.counterValue(ctx, admittedCounterKey(tenantID, eventID))
	if err != nil {
		return waitingroom.SessionStatus{}, fmt.Errorf("read admitted counter: %w", err)
	}

	return newSessionStatus(room, sessionID, position, admitted), nil
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

func newSessionStatus(room waitingroom.WaitingRoom, sessionID string, arrivalNumber, admitted int) waitingroom.SessionStatus {
	ahead := nonNegative(arrivalNumber - admitted - 1)
	remaining := nonNegative(arrivalNumber - admitted)

	return waitingroom.SessionStatus{
		TenantID:               room.TenantID,
		EventID:                room.EventID,
		SessionID:              sessionID,
		ArrivalNumber:          arrivalNumber,
		Position:               ahead + 1,
		Ahead:                  ahead,
		EstimatedWaitInSeconds: estimatedWaitInSeconds(remaining, room.AdmissionPolicy.AdmissionsPerSeconds),
		CanEnter:               ahead == 0,
	}
}

func estimatedWaitInSeconds(remaining, admissionsPerSecond int) int {
	if remaining <= 0 || admissionsPerSecond <= 0 {
		return 0
	}

	seconds := remaining / admissionsPerSecond
	if remaining%admissionsPerSecond != 0 {
		seconds++
	}

	return seconds
}

func nonNegative(value int) int {
	if value < 0 {
		return 0
	}

	return value
}

func generateAdmissionToken() (string, error) {
	token := make([]byte, admissionTokenBytes)
	if _, err := rand.Read(token); err != nil {
		return "", err
	}

	return base64.RawURLEncoding.EncodeToString(token), nil
}

func arrivalCounterKey(tenantID, eventID string) string {
	return roomKeyPrefix(tenantID, eventID) + ":arrival_counter"
}

func admittedCounterKey(tenantID, eventID string) string {
	return roomKeyPrefix(tenantID, eventID) + ":admitted_counter"
}

func sessionKey(tenantID, eventID, sessionID string) string {
	return roomKeyPrefix(tenantID, eventID) + ":session:" + sessionID
}

func roomKeyPrefix(tenantID, eventID string) string {
	return redisKeyPrefix + ":" + tenantID + ":" + eventID
}

func int64ToInt(value int64) (int, error) {
	if value > int64(maxInt) || value < int64(minInt) {
		return 0, fmt.Errorf("redis integer out of int range: %d", value)
	}

	return int(value), nil
}

const (
	maxInt = int(^uint(0) >> 1)
	minInt = -maxInt - 1
)
