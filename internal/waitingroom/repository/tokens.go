package repository

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/leandersteiner/go-waiting-room/internal/waitingroom"
)

const bearerTokenType = "Bearer"

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

func (r *RedisRepository) AdmissionTokenJWKSet() waitingroom.JWKSet {
	provider, ok := r.tokenIssuer.(waitingroom.AdmissionTokenKeySetProvider)
	if !ok {
		return waitingroom.JWKSet{}
	}

	return provider.AdmissionTokenJWKSet()
}
