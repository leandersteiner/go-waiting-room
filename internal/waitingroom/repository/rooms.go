package repository

import (
	"context"
	"fmt"
	"strconv"

	"github.com/leandersteiner/go-waiting-room/internal/waitingroom"
)

const (
	roomConfigQueueEnabledField  = "queue_enabled"
	roomConfigAdmissionRateField = "admission_rate"
	roomConfigMaxActiveField     = "max_active_admissions"
	roomConfigOfferTTLField      = "admission_offer_ttl_seconds"
	roomConfigTokenTTLField      = "token_ttl_seconds"
	roomConfigVersionField       = "version"

	scanCount = 1000
)

func (r *RedisRepository) GetRoom(ctx context.Context, tenantID, eventID string) (waitingroom.WaitingRoom, error) {
	room := waitingroom.NewDefaultRoom(tenantID, eventID)

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

func (r *RedisRepository) ListRooms(ctx context.Context) ([]waitingroom.RoomRef, error) {
	rooms := make(map[waitingroom.RoomRef]struct{})

	if err := r.scanRooms(ctx, roomConfigKeyPattern(), rooms); err != nil {
		return nil, err
	}
	if err := r.scanRooms(ctx, arrivalCounterKeyPattern(), rooms); err != nil {
		return nil, err
	}

	result := make([]waitingroom.RoomRef, 0, len(rooms))
	for room := range rooms {
		result = append(result, room)
	}

	return result, nil
}

func (r *RedisRepository) scanRooms(ctx context.Context, pattern string, rooms map[waitingroom.RoomRef]struct{}) error {
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
