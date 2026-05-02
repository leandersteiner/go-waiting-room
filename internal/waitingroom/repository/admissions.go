package repository

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/leandersteiner/go-waiting-room/internal/waitingroom"
	"github.com/redis/go-redis/v9"
)

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

	return waitingroom.NewAdmissionProgress(room, arrived, admitted, active, offers), nil
}

func (r *RedisRepository) AdvanceAdmission(ctx context.Context, tenantID, eventID string, request waitingroom.AdmissionAdvanceRequest) (waitingroom.AdmissionAdvanceResult, error) {
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
		request.AdmissionOfferTTLInSeconds = waitingroom.DefaultAdmissionOfferTTLInSeconds
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
		return waitingroom.AdmissionAdvanceResult{}, fmt.Errorf("advance admission: %w", err)
	}
	if len(result) != 6 {
		return waitingroom.AdmissionAdvanceResult{}, fmt.Errorf("advance admission: unexpected redis result length %d", len(result))
	}

	arrived, err := int64ToInt(result[0])
	if err != nil {
		return waitingroom.AdmissionAdvanceResult{}, err
	}
	previousAdmitted, err := int64ToInt(result[1])
	if err != nil {
		return waitingroom.AdmissionAdvanceResult{}, err
	}
	admitted, err := int64ToInt(result[2])
	if err != nil {
		return waitingroom.AdmissionAdvanceResult{}, err
	}
	active, err := int64ToInt(result[3])
	if err != nil {
		return waitingroom.AdmissionAdvanceResult{}, err
	}
	offers, err := int64ToInt(result[4])
	if err != nil {
		return waitingroom.AdmissionAdvanceResult{}, err
	}
	maxActive, err := int64ToInt(result[5])
	if err != nil {
		return waitingroom.AdmissionAdvanceResult{}, err
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
	advance := progress.AdmissionAdvanceResult(previousAdmitted)
	if advance.Advanced > 0 {
		_ = r.publishAdmissionProgress(ctx, progress)
	}

	return advance, nil
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
	*status = status.WithAdmissionAvailability(canEnter)

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

	return room.CanClaimAdmission(false, active, offers), nil
}
