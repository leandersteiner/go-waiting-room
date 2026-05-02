package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/leandersteiner/go-waiting-room/internal/waitingroom"
	"github.com/redis/go-redis/v9"
)

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
