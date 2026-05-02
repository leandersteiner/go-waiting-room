package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type counters struct {
	joined     atomic.Int64
	open       atomic.Int64
	events     atomic.Int64
	joinErrors atomic.Int64
	openErrors atomic.Int64
	readErrors atomic.Int64
}

func main() {
	baseURL := flag.String("base-url", env("BASE_URL", "http://localhost:8080"), "queue service base URL")
	tenantID := flag.String("tenant", env("TENANT", "load"), "tenant ID")
	eventID := flag.String("event", env("EVENT", "main"), "event ID")
	sessionPrefix := flag.String("session-prefix", env("SESSION_PREFIX", "sse"), "session ID prefix")
	sessions := flag.Int("sessions", envInt("SESSIONS", 1000), "number of SSE clients to hold open")
	hold := flag.Duration("hold", envDuration("HOLD", 5*time.Minute), "how long to hold streams open")
	joinWorkers := flag.Int("join-workers", envInt("JOIN_WORKERS", 100), "concurrent workers used to create sessions")
	openRate := flag.Int("open-rate", envInt("OPEN_RATE", 500), "maximum new SSE connections per second")
	reportEvery := flag.Duration("report-every", envDuration("REPORT_EVERY", 5*time.Second), "progress report interval")
	flag.Parse()

	if *sessions <= 0 {
		log.Fatal("-sessions must be greater than zero")
	}
	if *joinWorkers <= 0 {
		log.Fatal("-join-workers must be greater than zero")
	}
	if *openRate <= 0 {
		log.Fatal("-open-rate must be greater than zero")
	}

	client := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:        *sessions + *joinWorkers,
			MaxIdleConnsPerHost: *sessions + *joinWorkers,
			MaxConnsPerHost:     0,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	stats := &counters{}
	sessionIDs := make([]string, *sessions)
	for i := range sessionIDs {
		sessionIDs[i] = fmt.Sprintf("%s-%d", *sessionPrefix, i)
	}

	fmt.Printf("joining %d sessions against %s\n", *sessions, *baseURL)
	joinSessions(client, *baseURL, *tenantID, *eventID, sessionIDs, *joinWorkers, stats)
	if stats.joinErrors.Load() > 0 {
		log.Fatalf("join failed for %d sessions", stats.joinErrors.Load())
	}

	ctx, cancel := context.WithTimeout(context.Background(), *hold)
	defer cancel()

	var wg sync.WaitGroup
	startReporter(ctx, stats, *reportEvery)

	fmt.Printf("opening %d SSE streams at up to %d/s for %s\n", *sessions, *openRate, hold.String())
	interval := time.Second / time.Duration(*openRate)
	if interval <= 0 {
		interval = time.Nanosecond
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

openStreams:
	for _, sessionID := range sessionIDs {
		select {
		case <-ctx.Done():
			break openStreams
		case <-ticker.C:
		}

		wg.Add(1)
		go func(sessionID string) {
			defer wg.Done()
			holdStream(ctx, client, *baseURL, *tenantID, *eventID, sessionID, stats)
		}(sessionID)
	}

	<-ctx.Done()
	wg.Wait()

	fmt.Printf(
		"done joined=%d open=%d events=%d join_errors=%d open_errors=%d read_errors=%d\n",
		stats.joined.Load(),
		stats.open.Load(),
		stats.events.Load(),
		stats.joinErrors.Load(),
		stats.openErrors.Load(),
		stats.readErrors.Load(),
	)

	if stats.openErrors.Load() > 0 || stats.readErrors.Load() > 0 {
		os.Exit(1)
	}
}

func joinSessions(client *http.Client, baseURL, tenantID, eventID string, sessionIDs []string, workers int, stats *counters) {
	jobs := make(chan string)
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for sessionID := range jobs {
				if err := joinSession(client, baseURL, tenantID, eventID, sessionID); err != nil {
					stats.joinErrors.Add(1)
					continue
				}
				stats.joined.Add(1)
			}
		}()
	}

	for _, sessionID := range sessionIDs {
		jobs <- sessionID
	}
	close(jobs)
	wg.Wait()
}

func joinSession(client *http.Client, baseURL, tenantID, eventID, sessionID string) error {
	body, err := json.Marshal(map[string]string{"SessionID": sessionID})
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/v1/tenants/%s/events/%s/queue/join", strings.TrimRight(baseURL, "/"), tenantID, eventID)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("join %s returned %s", sessionID, resp.Status)
	}

	return nil
}

func holdStream(ctx context.Context, client *http.Client, baseURL, tenantID, eventID, sessionID string, stats *counters) {
	url := fmt.Sprintf(
		"%s/v1/tenants/%s/events/%s/queue/stream/%s",
		strings.TrimRight(baseURL, "/"),
		tenantID,
		eventID,
		sessionID,
	)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		stats.openErrors.Add(1)
		return
	}
	req.Header.Set("Accept", "text/event-stream")

	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() == nil {
			stats.openErrors.Add(1)
		}
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		stats.openErrors.Add(1)
		_, _ = io.Copy(io.Discard, resp.Body)
		return
	}

	stats.open.Add(1)
	defer stats.open.Add(-1)

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		if strings.HasPrefix(scanner.Text(), "event:") {
			stats.events.Add(1)
		}
	}

	if err := scanner.Err(); err != nil && ctx.Err() == nil {
		stats.readErrors.Add(1)
	}
}

func startReporter(ctx context.Context, stats *counters, every time.Duration) {
	if every <= 0 {
		return
	}

	go func() {
		ticker := time.NewTicker(every)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				fmt.Printf(
					"progress joined=%d open=%d events=%d join_errors=%d open_errors=%d read_errors=%d\n",
					stats.joined.Load(),
					stats.open.Load(),
					stats.events.Load(),
					stats.joinErrors.Load(),
					stats.openErrors.Load(),
					stats.readErrors.Load(),
				)
			}
		}
	}()
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	var parsed int
	if _, err := fmt.Sscanf(value, "%d", &parsed); err != nil {
		return fallback
	}
	return parsed
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}
