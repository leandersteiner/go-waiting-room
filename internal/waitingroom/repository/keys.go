package repository

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/leandersteiner/go-waiting-room/internal/waitingroom"
)

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

func roomRefFromKey(key string) (waitingroom.RoomRef, bool) {
	parts := strings.Split(key, ":")
	if len(parts) < 4 || parts[0] != redisKeyPrefix {
		return waitingroom.RoomRef{}, false
	}

	return waitingroom.RoomRef{
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
