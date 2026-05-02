package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/leandersteiner/go-waiting-room/internal/waitingroom"
	"github.com/redis/go-redis/v9"
)

const (
	redisKeyPrefix = "waitroom"

	defaultAdmissionsPerSecond = 250
	defaultTokenTTLInSeconds   = 900
	defaultMaxActiveAdmissions = 250
	defaultOfferTTLInSeconds   = 60
	defaultQueueEnabled        = true

	bearerTokenType = "Bearer"

	roomConfigQueueEnabledField  = "queue_enabled"
	roomConfigAdmissionRateField = "admission_rate"
	roomConfigMaxActiveField     = "max_active_admissions"
	roomConfigOfferTTLField      = "admission_offer_ttl_seconds"
	roomConfigTokenTTLField      = "token_ttl_seconds"
	roomConfigVersionField       = "version"

	workerLockKey = redisKeyPrefix + ":worker:lock"
	scanCount     = 1000
)

var (
	ErrRoomNotFound           = errors.New("room not found")
	ErrSessionNotFound        = errors.New("session not found")
	ErrSessionNotAdmitted     = errors.New("session not admitted")
	ErrAdmissionCapacityFull  = errors.New("admission capacity full")
	ErrAdmissionLeaseMismatch = errors.New("admission lease does not match session")
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
local existingToken = redis.call("GET", KEYS[3])
if existingToken then
	local ttl = redis.call("TTL", KEYS[3])
	if ttl < 0 then
		ttl = 0
	end
	local existingLease = redis.call("GET", KEYS[6]) or ""
	return {3, existingToken, ttl, existingLease}
end

local position = redis.call("GET", KEYS[1])
if not position then
	return {0, "", 0, ""}
end

position = tonumber(position)
if not position then
	return redis.error_reply("invalid session position")
end

local admitted = tonumber(redis.call("GET", KEYS[2]) or "0")
if not admitted then
	return redis.error_reply("invalid admitted counter")
end

if position > admitted then
	return {1, "", 0, ""}
end

local now = tonumber(ARGV[3])
if not now then
	return redis.error_reply("invalid current time")
end
local maxActive = tonumber(ARGV[7])
if not maxActive then
	return redis.error_reply("invalid max active admissions")
end

local offerScore = redis.call("ZSCORE", KEYS[4], tostring(position))
local hasOffer = 0
if offerScore then
	offerScore = tonumber(offerScore)
	if not offerScore then
		return redis.error_reply("invalid admission offer score")
	end
	if offerScore > now then
		hasOffer = 1
	else
		redis.call("ZREM", KEYS[4], tostring(position))
	end
end

redis.call("ZREMRANGEBYSCORE", KEYS[5], "-inf", now)
redis.call("ZREMRANGEBYSCORE", KEYS[4], "-inf", now)

if hasOffer == 0 then
	local active = redis.call("ZCARD", KEYS[5])
	local offers = redis.call("ZCARD", KEYS[4])
	local available = maxActive - active - offers
	if available <= 0 then
		return {4, "", 0, ""}
	end
end

redis.call("SET", KEYS[3], ARGV[1], "EX", ARGV[2])
redis.call("ZREM", KEYS[4], tostring(position))
redis.call("ZADD", KEYS[5], ARGV[4], ARGV[5])
redis.call("SET", KEYS[6], ARGV[5], "EX", ARGV[2])
redis.call("SET", KEYS[7], ARGV[6], "EX", ARGV[2])
redis.call("DEL", KEYS[1])
return {2, ARGV[1], tonumber(ARGV[2]), ARGV[5]}
`)

var advanceAdmissionScript = redis.NewScript(`
local arrived = tonumber(redis.call("GET", KEYS[1]) or "0")
if not arrived then
	return redis.error_reply("invalid arrival counter")
end

local admitted = tonumber(redis.call("GET", KEYS[2]) or "0")
if not admitted then
	return redis.error_reply("invalid admitted counter")
end

local now = tonumber(ARGV[2])
if not now then
	return redis.error_reply("invalid current time")
end
local offerExpiresAt = tonumber(ARGV[3])
if not offerExpiresAt then
	return redis.error_reply("invalid offer expiry")
end
local maxActive = tonumber(ARGV[4])
if not maxActive then
	return redis.error_reply("invalid max active admissions")
end
if maxActive < 0 then
	return redis.error_reply("negative max active admissions")
end

local increment = tonumber(ARGV[1])
if not increment then
	return redis.error_reply("invalid admission increment")
end
if increment < 0 then
	return redis.error_reply("negative admission increment")
end

redis.call("ZREMRANGEBYSCORE", KEYS[3], "-inf", now)
redis.call("ZREMRANGEBYSCORE", KEYS[4], "-inf", now)

local active = redis.call("ZCARD", KEYS[3])
local offers = redis.call("ZCARD", KEYS[4])
local available = maxActive - active - offers
if available < 0 then
	available = 0
end

local queued = arrived - admitted
if queued < 0 then
	queued = 0
end

local admit = increment
if admit > available then
	admit = available
end
if admit > queued then
	admit = queued
end

local target = admitted + admit
if target > admitted then
	for arrival = admitted + 1, target do
		redis.call("ZADD", KEYS[4], offerExpiresAt, tostring(arrival))
	end
	redis.call("SET", KEYS[2], target)
end

active = redis.call("ZCARD", KEYS[3])
offers = redis.call("ZCARD", KEYS[4])
return {arrived, admitted, target, active, offers, maxActive}
`)

var releaseAdmissionScript = redis.NewScript(`
local lease = redis.call("GET", KEYS[3])
if lease and lease ~= ARGV[1] then
	return {-1}
end
local session = redis.call("GET", KEYS[4])
if session and session ~= ARGV[2] then
	return {-1}
end

local removed = redis.call("ZREM", KEYS[1], ARGV[1])
redis.call("DEL", KEYS[2])
redis.call("DEL", KEYS[3])
redis.call("DEL", KEYS[4])

return {removed}
`)

var acquireWorkerLockScript = redis.NewScript(`
local owner = redis.call("GET", KEYS[1])
if not owner then
    local ok = redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2], "NX")
    if ok then
        return 1
    end
    return 0
end

if owner == ARGV[1] then
	redis.call("PEXPIRE", KEYS[1], ARGV[2])
	return 1
end

return 0
`)

type RedisRepository struct {
	rdb            *redis.Client
	tokenIssuer    waitingroom.AdmissionTokenIssuer
	subscriptionMu sync.Mutex
	subscriptions  map[string]*sharedAdmissionProgressSubscription
}

type RoomRef struct {
	TenantID string
	EventID  string
}

type AdvanceAdmissionResult struct {
	Progress waitingroom.AdmissionProgress
	Advanced int
}

type AdvanceAdmissionRequest struct {
	Amount                     int
	MaxActiveAdmissions        int
	AdmissionOfferTTLInSeconds int
}

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

func (r *RedisRepository) IssueAdmissionToken(ctx context.Context, tenantID, eventID, sessionID string) (waitingroom.AdmissionToken, error) {
	room, err := r.GetRoom(ctx, tenantID, eventID)
	if err != nil {
		return waitingroom.AdmissionToken{}, fmt.Errorf("get waiting room: %w", err)
	}
	if !room.QueueEnabled {
		return waitingroom.AdmissionToken{}, ErrRoomNotFound
	}

	tokenID, err := waitingroom.NewAdmissionTokenID()
	if err != nil {
		return waitingroom.AdmissionToken{}, fmt.Errorf("generate admission token id: %w", err)
	}

	now := time.Now().UTC()
	token, err := r.tokenIssuer.IssueAdmissionToken(waitingroom.AdmissionTokenClaims{
		TenantID:  tenantID,
		EventID:   eventID,
		SessionID: sessionID,
		JWTID:     tokenID,
		IssuedAt:  now,
		NotBefore: now,
		ExpiresAt: now.Add(time.Duration(room.TokenPolicy.TokenTTLInSeconds) * time.Second),
	})
	if err != nil {
		return waitingroom.AdmissionToken{}, fmt.Errorf("generate admission token: %w", err)
	}

	keys := []string{
		sessionKey(tenantID, eventID, sessionID),
		admittedCounterKey(tenantID, eventID),
		tokenIssuedKey(tenantID, eventID, sessionID),
		admissionOffersKey(tenantID, eventID),
		activeAdmissionsKey(tenantID, eventID),
		sessionLeaseKey(tenantID, eventID, sessionID),
		leaseSessionKey(tenantID, eventID, tokenID),
	}
	result, err := issueAdmissionTokenScript.Run(
		ctx,
		r.rdb,
		keys,
		token,
		strconv.Itoa(room.TokenPolicy.TokenTTLInSeconds),
		strconv.FormatInt(now.UnixMilli(), 10),
		strconv.FormatInt(now.Add(time.Duration(room.TokenPolicy.TokenTTLInSeconds)*time.Second).UnixMilli(), 10),
		tokenID,
		sessionID,
		strconv.Itoa(room.AdmissionPolicy.MaxActiveAdmissions),
	).Slice()
	if err != nil {
		return waitingroom.AdmissionToken{}, fmt.Errorf("issue admission token: %w", err)
	}
	if len(result) != 4 {
		return waitingroom.AdmissionToken{}, fmt.Errorf("issue admission token: unexpected redis result length %d", len(result))
	}

	status, err := redisInt(result[0])
	if err != nil {
		return waitingroom.AdmissionToken{}, fmt.Errorf("issue admission token: invalid redis status: %w", err)
	}

	switch status {
	case 0:
		return waitingroom.AdmissionToken{}, ErrSessionNotFound
	case 1:
		return waitingroom.AdmissionToken{}, ErrSessionNotAdmitted
	case 4:
		return waitingroom.AdmissionToken{}, ErrAdmissionCapacityFull
	case 2, 3:
		issuedToken, ok := result[1].(string)
		if !ok {
			return waitingroom.AdmissionToken{}, fmt.Errorf("issue admission token: invalid token result %T", result[1])
		}
		expiresIn, err := redisInt(result[2])
		if err != nil {
			return waitingroom.AdmissionToken{}, fmt.Errorf("issue admission token: invalid token ttl: %w", err)
		}
		issuedTokenID, ok := result[3].(string)
		if !ok {
			return waitingroom.AdmissionToken{}, fmt.Errorf("issue admission token: invalid token id result %T", result[3])
		}

		return waitingroom.AdmissionToken{
			TenantID:  tenantID,
			EventID:   eventID,
			SessionID: sessionID,
			TokenID:   issuedTokenID,
			TokenType: bearerTokenType,
			Token:     issuedToken,
			ExpiresIn: int(expiresIn),
		}, nil
	default:
		return waitingroom.AdmissionToken{}, fmt.Errorf("issue admission token: unexpected redis status %d", status)
	}
}

func (r *RedisRepository) ReleaseAdmission(ctx context.Context, tenantID, eventID, sessionID, tokenID string) (bool, error) {
	if strings.TrimSpace(sessionID) == "" {
		return false, ErrSessionNotFound
	}
	if strings.TrimSpace(tokenID) == "" {
		return false, ErrAdmissionLeaseMismatch
	}

	keys := []string{
		activeAdmissionsKey(tenantID, eventID),
		tokenIssuedKey(tenantID, eventID, sessionID),
		sessionLeaseKey(tenantID, eventID, sessionID),
		leaseSessionKey(tenantID, eventID, tokenID),
	}
	result, err := releaseAdmissionScript.Run(ctx, r.rdb, keys, tokenID, sessionID).Int64Slice()
	if err != nil {
		return false, fmt.Errorf("release admission: %w", err)
	}
	if len(result) != 1 {
		return false, fmt.Errorf("release admission: unexpected redis result length %d", len(result))
	}
	if result[0] < 0 {
		return false, ErrAdmissionLeaseMismatch
	}

	released := result[0] > 0
	if released {
		progress, err := r.GetAdmissionProgress(ctx, tenantID, eventID)
		if err == nil {
			_ = r.publishAdmissionProgress(ctx, progress)
		}
	}

	return released, nil
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

	status := newSessionStatus(room, sessionID, position, admitted)
	if err := r.applyAdmissionAvailability(ctx, room, &status); err != nil {
		return waitingroom.SessionStatus{}, err
	}

	return status, nil
}

func (r *RedisRepository) GetRoom(ctx context.Context, tenantID, eventID string) (waitingroom.WaitingRoom, error) {
	room := waitingroom.WaitingRoom{
		TenantID:     tenantID,
		EventID:      eventID,
		QueueEnabled: defaultQueueEnabled,
		AdmissionPolicy: waitingroom.AdmissionPolicy{
			AdmissionsPerSeconds:       defaultAdmissionsPerSecond,
			MaxActiveAdmissions:        defaultMaxActiveAdmissions,
			AdmissionOfferTTLInSeconds: defaultOfferTTLInSeconds,
		},
		TokenPolicy: waitingroom.TokenPolicy{
			TokenTTLInSeconds: defaultTokenTTLInSeconds,
		},
	}

	values, err := r.rdb.HGetAll(ctx, roomConfigKey(tenantID, eventID)).Result()
	if err != nil {
		return waitingroom.WaitingRoom{}, fmt.Errorf("read room config: %w", err)
	}
	if len(values) == 0 {
		return room, nil
	}

	if value, ok := values[roomConfigQueueEnabledField]; ok {
		queueEnabled, err := strconv.ParseBool(value)
		if err != nil {
			return waitingroom.WaitingRoom{}, fmt.Errorf("parse %s: %w", roomConfigQueueEnabledField, err)
		}
		room.QueueEnabled = queueEnabled
	}

	if value, ok := values[roomConfigAdmissionRateField]; ok {
		admissionRate, err := strconv.Atoi(value)
		if err != nil {
			return waitingroom.WaitingRoom{}, fmt.Errorf("parse %s: %w", roomConfigAdmissionRateField, err)
		}
		if admissionRate < 0 {
			return waitingroom.WaitingRoom{}, fmt.Errorf("%s cannot be negative", roomConfigAdmissionRateField)
		}
		room.AdmissionPolicy.AdmissionsPerSeconds = admissionRate
	}

	if value, ok := values[roomConfigMaxActiveField]; ok {
		maxActive, err := strconv.Atoi(value)
		if err != nil {
			return waitingroom.WaitingRoom{}, fmt.Errorf("parse %s: %w", roomConfigMaxActiveField, err)
		}
		if maxActive < 0 {
			return waitingroom.WaitingRoom{}, fmt.Errorf("%s cannot be negative", roomConfigMaxActiveField)
		}
		room.AdmissionPolicy.MaxActiveAdmissions = maxActive
	}

	if value, ok := values[roomConfigOfferTTLField]; ok {
		offerTTL, err := strconv.Atoi(value)
		if err != nil {
			return waitingroom.WaitingRoom{}, fmt.Errorf("parse %s: %w", roomConfigOfferTTLField, err)
		}
		if offerTTL <= 0 {
			return waitingroom.WaitingRoom{}, fmt.Errorf("%s must be positive", roomConfigOfferTTLField)
		}
		room.AdmissionPolicy.AdmissionOfferTTLInSeconds = offerTTL
	}

	if value, ok := values[roomConfigTokenTTLField]; ok {
		tokenTTL, err := strconv.Atoi(value)
		if err != nil {
			return waitingroom.WaitingRoom{}, fmt.Errorf("parse %s: %w", roomConfigTokenTTLField, err)
		}
		if tokenTTL <= 0 {
			return waitingroom.WaitingRoom{}, fmt.Errorf("%s must be positive", roomConfigTokenTTLField)
		}
		room.TokenPolicy.TokenTTLInSeconds = tokenTTL
	}

	if value, ok := values[roomConfigVersionField]; ok {
		version, err := strconv.Atoi(value)
		if err != nil {
			return waitingroom.WaitingRoom{}, fmt.Errorf("parse %s: %w", roomConfigVersionField, err)
		}
		room.Version = version
	}

	return room, nil
}

func (r *RedisRepository) GetAdmissionProgress(ctx context.Context, tenantID, eventID string) (waitingroom.AdmissionProgress, error) {
	room, err := r.GetRoom(ctx, tenantID, eventID)
	if err != nil {
		return waitingroom.AdmissionProgress{}, fmt.Errorf("get waiting room: %w", err)
	}

	arrived, err := r.counterValue(ctx, arrivalCounterKey(tenantID, eventID))
	if err != nil {
		return waitingroom.AdmissionProgress{}, fmt.Errorf("read arrival counter: %w", err)
	}

	admitted, err := r.counterValue(ctx, admittedCounterKey(tenantID, eventID))
	if err != nil {
		return waitingroom.AdmissionProgress{}, fmt.Errorf("read admitted counter: %w", err)
	}

	active, offers, err := r.admissionLeaseCounts(ctx, tenantID, eventID, time.Now())
	if err != nil {
		return waitingroom.AdmissionProgress{}, err
	}

	return waitingroom.AdmissionProgress{
		TenantID:            tenantID,
		EventID:             eventID,
		ArrivalCounter:      arrived,
		AdmittedCounter:     admitted,
		ActiveAdmissions:    active,
		AdmissionOffers:     offers,
		MaxActiveAdmissions: room.AdmissionPolicy.MaxActiveAdmissions,
	}, nil
}

func (r *RedisRepository) AdmissionTokenJWKSet() waitingroom.JWKSet {
	provider, ok := r.tokenIssuer.(waitingroom.AdmissionTokenKeySetProvider)
	if !ok {
		return waitingroom.JWKSet{}
	}

	return provider.AdmissionTokenJWKSet()
}

func (r *RedisRepository) AdvanceAdmission(ctx context.Context, tenantID, eventID string, request AdvanceAdmissionRequest) (AdvanceAdmissionResult, error) {
	keys := []string{
		arrivalCounterKey(tenantID, eventID),
		admittedCounterKey(tenantID, eventID),
		activeAdmissionsKey(tenantID, eventID),
		admissionOffersKey(tenantID, eventID),
	}

	if request.MaxActiveAdmissions < 0 {
		request.MaxActiveAdmissions = 0
	}
	if request.AdmissionOfferTTLInSeconds <= 0 {
		request.AdmissionOfferTTLInSeconds = defaultOfferTTLInSeconds
	}

	now := time.Now()
	result, err := advanceAdmissionScript.Run(
		ctx,
		r.rdb,
		keys,
		strconv.Itoa(request.Amount),
		strconv.FormatInt(now.UnixMilli(), 10),
		strconv.FormatInt(now.Add(time.Duration(request.AdmissionOfferTTLInSeconds)*time.Second).UnixMilli(), 10),
		strconv.Itoa(request.MaxActiveAdmissions),
	).Int64Slice()
	if err != nil {
		return AdvanceAdmissionResult{}, fmt.Errorf("advance admission: %w", err)
	}
	if len(result) != 6 {
		return AdvanceAdmissionResult{}, fmt.Errorf("advance admission: unexpected redis result length %d", len(result))
	}

	arrived, err := int64ToInt(result[0])
	if err != nil {
		return AdvanceAdmissionResult{}, err
	}
	previousAdmitted, err := int64ToInt(result[1])
	if err != nil {
		return AdvanceAdmissionResult{}, err
	}
	admitted, err := int64ToInt(result[2])
	if err != nil {
		return AdvanceAdmissionResult{}, err
	}
	active, err := int64ToInt(result[3])
	if err != nil {
		return AdvanceAdmissionResult{}, err
	}
	offers, err := int64ToInt(result[4])
	if err != nil {
		return AdvanceAdmissionResult{}, err
	}
	maxActive, err := int64ToInt(result[5])
	if err != nil {
		return AdvanceAdmissionResult{}, err
	}

	progress := waitingroom.AdmissionProgress{
		TenantID:            tenantID,
		EventID:             eventID,
		ArrivalCounter:      arrived,
		AdmittedCounter:     admitted,
		ActiveAdmissions:    active,
		AdmissionOffers:     offers,
		MaxActiveAdmissions: maxActive,
	}
	advanced := admitted - previousAdmitted
	if advanced > 0 {
		_ = r.publishAdmissionProgress(ctx, progress)
	}

	return AdvanceAdmissionResult{
		Progress: progress,
		Advanced: advanced,
	}, nil
}

func (r *RedisRepository) TryAcquireWorkerLock(ctx context.Context, owner string, ttl time.Duration) (bool, error) {
	if strings.TrimSpace(owner) == "" {
		return false, errors.New("worker lock owner is required")
	}
	if ttl <= 0 {
		return false, errors.New("worker lock ttl must be positive")
	}

	acquired, err := acquireWorkerLockScript.Run(
		ctx,
		r.rdb,
		[]string{workerLockKey},
		owner,
		strconv.FormatInt(ttl.Milliseconds(), 10),
	).Bool()
	if err != nil {
		return false, fmt.Errorf("acquire worker lock: %w", err)
	}

	return acquired, nil
}

func (r *RedisRepository) ListRooms(ctx context.Context) ([]RoomRef, error) {
	rooms := make(map[RoomRef]struct{})

	if err := r.scanRooms(ctx, roomConfigKeyPattern(), rooms); err != nil {
		return nil, err
	}
	if err := r.scanRooms(ctx, arrivalCounterKeyPattern(), rooms); err != nil {
		return nil, err
	}

	result := make([]RoomRef, 0, len(rooms))
	for room := range rooms {
		result = append(result, room)
	}

	return result, nil
}

func (r *RedisRepository) SubscribeAdmissionProgress(ctx context.Context, tenantID, eventID string) (waitingroom.AdmissionProgressSubscription, error) {
	channelName := admissionProgressChannel(tenantID, eventID)

	r.subscriptionMu.Lock()
	defer r.subscriptionMu.Unlock()

	shared := r.subscriptions[channelName]
	if shared == nil || shared.isClosed() {
		var err error
		shared, err = r.newSharedAdmissionProgressSubscription(ctx, channelName)
		if err != nil {
			return nil, err
		}
		r.subscriptions[channelName] = shared
	}

	id, updates, err := shared.addSubscriber()
	if err != nil {
		delete(r.subscriptions, channelName)
		return nil, err
	}

	return &redisAdmissionProgressSubscription{
		shared:  shared,
		id:      id,
		updates: updates,
	}, nil
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

	status := newSessionStatus(room, sessionID, position, admitted)
	if err := r.applyAdmissionAvailability(ctx, room, &status); err != nil {
		return waitingroom.SessionStatus{}, err
	}

	return status, nil
}

func (r *RedisRepository) scanRooms(ctx context.Context, pattern string, rooms map[RoomRef]struct{}) error {
	var cursor uint64
	for {
		keys, nextCursor, err := r.rdb.Scan(ctx, cursor, pattern, scanCount).Result()
		if err != nil {
			return fmt.Errorf("scan rooms: %w", err)
		}

		for _, key := range keys {
			room, ok := roomRefFromKey(key)
			if ok {
				rooms[room] = struct{}{}
			}
		}

		if nextCursor == 0 {
			return nil
		}
		cursor = nextCursor
	}
}

func (r *RedisRepository) publishAdmissionProgress(ctx context.Context, progress waitingroom.AdmissionProgress) error {
	payload, err := json.Marshal(progress)
	if err != nil {
		return fmt.Errorf("marshal admission progress: %w", err)
	}

	if err := r.rdb.Publish(ctx, admissionProgressChannel(progress.TenantID, progress.EventID), payload).Err(); err != nil {
		return fmt.Errorf("publish admission progress: %w", err)
	}

	return nil
}

func (r *RedisRepository) newSharedAdmissionProgressSubscription(ctx context.Context, channelName string) (*sharedAdmissionProgressSubscription, error) {
	pubsub := r.rdb.Subscribe(context.Background(), channelName)
	if _, err := pubsub.Receive(ctx); err != nil {
		_ = pubsub.Close()
		return nil, fmt.Errorf("subscribe admission progress: %w", err)
	}

	shared := &sharedAdmissionProgressSubscription{
		repo:        r,
		channelName: channelName,
		pubsub:      pubsub,
		subscribers: make(map[int]chan waitingroom.AdmissionProgress),
	}
	go shared.run()

	return shared, nil
}

func (r *RedisRepository) removeSharedAdmissionProgressSubscription(channelName string, shared *sharedAdmissionProgressSubscription) {
	r.subscriptionMu.Lock()
	defer r.subscriptionMu.Unlock()

	if r.subscriptions[channelName] == shared {
		delete(r.subscriptions, channelName)
	}
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

func (r *RedisRepository) admissionLeaseCounts(ctx context.Context, tenantID, eventID string, now time.Time) (int, int, error) {
	activeKey := activeAdmissionsKey(tenantID, eventID)
	offersKey := admissionOffersKey(tenantID, eventID)
	nowMillis := strconv.FormatInt(now.UnixMilli(), 10)

	if err := r.rdb.ZRemRangeByScore(ctx, activeKey, "-inf", nowMillis).Err(); err != nil {
		return 0, 0, fmt.Errorf("clean active admissions: %w", err)
	}
	if err := r.rdb.ZRemRangeByScore(ctx, offersKey, "-inf", nowMillis).Err(); err != nil {
		return 0, 0, fmt.Errorf("clean admission offers: %w", err)
	}

	active, err := r.rdb.ZCard(ctx, activeKey).Result()
	if err != nil {
		return 0, 0, fmt.Errorf("count active admissions: %w", err)
	}
	offers, err := r.rdb.ZCard(ctx, offersKey).Result()
	if err != nil {
		return 0, 0, fmt.Errorf("count admission offers: %w", err)
	}

	activeCount, err := int64ToInt(active)
	if err != nil {
		return 0, 0, err
	}
	offerCount, err := int64ToInt(offers)
	if err != nil {
		return 0, 0, err
	}

	return activeCount, offerCount, nil
}

func (r *RedisRepository) applyAdmissionAvailability(ctx context.Context, room waitingroom.WaitingRoom, status *waitingroom.SessionStatus) error {
	if !status.CanEnter {
		return nil
	}

	canEnter, err := r.sessionCanClaimAdmission(ctx, room, status.ArrivalNumber, time.Now())
	if err != nil {
		return err
	}
	status.CanEnter = canEnter

	return nil
}

func (r *RedisRepository) sessionCanClaimAdmission(ctx context.Context, room waitingroom.WaitingRoom, arrivalNumber int, now time.Time) (bool, error) {
	if room.AdmissionPolicy.MaxActiveAdmissions <= 0 {
		return false, nil
	}

	offerScore, err := r.rdb.ZScore(ctx, admissionOffersKey(room.TenantID, room.EventID), strconv.Itoa(arrivalNumber)).Result()
	if err == nil {
		if offerScore > float64(now.UnixMilli()) {
			return true, nil
		}
		_ = r.rdb.ZRem(ctx, admissionOffersKey(room.TenantID, room.EventID), strconv.Itoa(arrivalNumber)).Err()
	} else if !errors.Is(err, redis.Nil) {
		return false, fmt.Errorf("read admission offer: %w", err)
	}

	active, offers, err := r.admissionLeaseCounts(ctx, room.TenantID, room.EventID, now)
	if err != nil {
		return false, err
	}

	return active+offers < room.AdmissionPolicy.MaxActiveAdmissions, nil
}

func newSessionStatus(room waitingroom.WaitingRoom, sessionID string, arrivalNumber, admitted int) waitingroom.SessionStatus {
	remaining := nonNegative(arrivalNumber - admitted)
	ahead := nonNegative(remaining - 1)

	return waitingroom.SessionStatus{
		TenantID:               room.TenantID,
		EventID:                room.EventID,
		SessionID:              sessionID,
		ArrivalNumber:          arrivalNumber,
		Position:               remaining,
		Ahead:                  ahead,
		EstimatedWaitInSeconds: estimatedWaitInSeconds(remaining, room.AdmissionPolicy.AdmissionsPerSeconds),
		CanEnter:               arrivalNumber <= admitted,
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

type redisAdmissionProgressSubscription struct {
	shared  *sharedAdmissionProgressSubscription
	id      int
	updates <-chan waitingroom.AdmissionProgress
	once    sync.Once
}

func (s *redisAdmissionProgressSubscription) Updates() <-chan waitingroom.AdmissionProgress {
	return s.updates
}

func (s *redisAdmissionProgressSubscription) Close() error {
	s.once.Do(func() {
		s.shared.removeSubscriber(s.id)
	})
	return nil
}

type sharedAdmissionProgressSubscription struct {
	repo        *RedisRepository
	channelName string
	pubsub      *redis.PubSub

	mu          sync.Mutex
	nextID      int
	subscribers map[int]chan waitingroom.AdmissionProgress
}

func (s *sharedAdmissionProgressSubscription) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.subscribers == nil
}

func (s *sharedAdmissionProgressSubscription) addSubscriber() (int, <-chan waitingroom.AdmissionProgress, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.subscribers == nil {
		return 0, nil, errors.New("admission progress subscription is closed")
	}

	s.nextID++
	id := s.nextID
	updates := make(chan waitingroom.AdmissionProgress, 16)
	s.subscribers[id] = updates

	return id, updates, nil
}

func (s *sharedAdmissionProgressSubscription) removeSubscriber(id int) {
	s.mu.Lock()
	if s.subscribers == nil {
		s.mu.Unlock()
		return
	}

	updates, ok := s.subscribers[id]
	if ok {
		delete(s.subscribers, id)
		close(updates)
	}
	empty := len(s.subscribers) == 0
	if empty {
		s.subscribers = nil
	}
	s.mu.Unlock()

	if empty {
		_ = s.pubsub.Close()
		s.repo.removeSharedAdmissionProgressSubscription(s.channelName, s)
	}
}

func (s *sharedAdmissionProgressSubscription) run() {
	channel := s.pubsub.Channel()
	for message := range channel {
		var progress waitingroom.AdmissionProgress
		if err := json.Unmarshal([]byte(message.Payload), &progress); err != nil {
			continue
		}

		s.broadcast(progress)
	}

	s.closeAll()
}

func (s *sharedAdmissionProgressSubscription) broadcast(progress waitingroom.AdmissionProgress) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, updates := range s.subscribers {
		select {
		case updates <- progress:
		default:
		}
	}
}

func (s *sharedAdmissionProgressSubscription) closeAll() {
	s.mu.Lock()
	if s.subscribers == nil {
		s.mu.Unlock()
		return
	}

	for _, updates := range s.subscribers {
		close(updates)
	}
	s.subscribers = nil
	s.mu.Unlock()

	s.repo.removeSharedAdmissionProgressSubscription(s.channelName, s)
}

func arrivalCounterKey(tenantID, eventID string) string {
	return roomKeyPrefix(tenantID, eventID) + ":arrival_counter"
}

func arrivalCounterKeyPattern() string {
	return redisKeyPrefix + ":*:*:arrival_counter"
}

func admittedCounterKey(tenantID, eventID string) string {
	return roomKeyPrefix(tenantID, eventID) + ":admitted_counter"
}

func admissionOffersKey(tenantID, eventID string) string {
	return roomKeyPrefix(tenantID, eventID) + ":admission_offers"
}

func activeAdmissionsKey(tenantID, eventID string) string {
	return roomKeyPrefix(tenantID, eventID) + ":active_admissions"
}

func sessionKey(tenantID, eventID, sessionID string) string {
	return roomKeyPrefix(tenantID, eventID) + ":session:" + sessionID
}

func roomConfigKey(tenantID, eventID string) string {
	return roomKeyPrefix(tenantID, eventID) + ":config"
}

func roomConfigKeyPattern() string {
	return redisKeyPrefix + ":*:*:config"
}

func tokenIssuedKey(tenantID, eventID, sessionID string) string {
	return roomKeyPrefix(tenantID, eventID) + ":token_issued:" + sessionID
}

func sessionLeaseKey(tenantID, eventID, sessionID string) string {
	return roomKeyPrefix(tenantID, eventID) + ":session_lease:" + sessionID
}

func leaseSessionKey(tenantID, eventID, tokenID string) string {
	return roomKeyPrefix(tenantID, eventID) + ":lease_session:" + tokenID
}

func admissionProgressChannel(tenantID, eventID string) string {
	return roomKeyPrefix(tenantID, eventID) + ":admission_progress"
}

func roomKeyPrefix(tenantID, eventID string) string {
	return redisKeyPrefix + ":" + tenantID + ":" + eventID
}

func roomRefFromKey(key string) (RoomRef, bool) {
	parts := strings.Split(key, ":")
	if len(parts) < 4 || parts[0] != redisKeyPrefix {
		return RoomRef{}, false
	}

	return RoomRef{
		TenantID: parts[1],
		EventID:  parts[2],
	}, true
}

func redisInt(value any) (int64, error) {
	switch value := value.(type) {
	case int64:
		return value, nil
	case int:
		return int64(value), nil
	case string:
		return strconv.ParseInt(value, 10, 64)
	default:
		return 0, fmt.Errorf("unexpected type %T", value)
	}
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
