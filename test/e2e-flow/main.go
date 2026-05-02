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
	joined       atomic.Int64
	subscribed   atomic.Int64
	activeStream atomic.Int64
	events       atomic.Int64
	tokens       atomic.Int64
	joinErrors   atomic.Int64
	streamErrors atomic.Int64
	parseErrors  atomic.Int64
	tokenErrors  atomic.Int64
}

type streamStatus struct {
	ArrivalNumber          int  `json:"arrivalNumber"`
	Position               int  `json:"position"`
	Ahead                  int  `json:"ahead"`
	EstimatedWaitInSeconds int  `json:"estimatedWaitInSeconds"`
	CanEnter               bool `json:"canEnter"`
}

type tokenResponse struct {
	TokenType string
	Token     string
	ExpiresIn int
}

func main() {
	baseURL := flag.String("base-url", env("BASE_URL", "http://localhost:8080"), "queue service base URL")
	tenantID := flag.String("tenant", env("TENANT", "load"), "tenant ID")
	eventID := flag.String("event", env("EVENT", "main"), "event ID")
	sessionPrefix := flag.String("session-prefix", env("SESSION_PREFIX", "flow"), "session ID prefix")
	sessions := flag.Int("sessions", envInt("SESSIONS", 100), "number of clients to run through the full flow")
	timeout := flag.Duration("timeout", envDuration("TIMEOUT", 10*time.Minute), "maximum test duration")
	joinWorkers := flag.Int("join-workers", envInt("JOIN_WORKERS", 100), "concurrent workers used to create sessions")
	openRate := flag.Int("open-rate", envInt("OPEN_RATE", 500), "maximum new SSE connections per second")
	gateStreams := flag.Bool("gate-streams", envBool("GATE_STREAMS", false), "wait until every stream is subscribed before processing stream events")
	reportEvery := flag.Duration("report-every", envDuration("REPORT_EVERY", 1*time.Second), "progress report interval")
	flag.Parse()

	if *sessions <= 0 {
		log.Fatal("-sessions must be greater than zero")
	}
	if *timeout <= 0 {
		log.Fatal("-timeout must be greater than zero")
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

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	startReporter(ctx, stats, *reportEvery)

	streamGate := make(chan struct{})
	if !*gateStreams {
		close(streamGate)
	}

	var wg sync.WaitGroup
	fmt.Printf("opening %d SSE streams at up to %d/s\n", *sessions, *openRate)
	openStreams(ctx, client, *baseURL, *tenantID, *eventID, sessionIDs, *openRate, streamGate, stats, int64(*sessions), cancel, &wg)

	if *gateStreams {
		fmt.Printf("waiting for all streams to subscribe before processing events\n")
		waitForSubscriptions(ctx, stats, int64(*sessions))
		close(streamGate)
	}

	wg.Wait()

	fmt.Printf(
		"done joined=%d subscribed=%d active_streams=%d events=%d tokens=%d join_errors=%d stream_errors=%d parse_errors=%d token_errors=%d\n",
		stats.joined.Load(),
		stats.subscribed.Load(),
		stats.activeStream.Load(),
		stats.events.Load(),
		stats.tokens.Load(),
		stats.joinErrors.Load(),
		stats.streamErrors.Load(),
		stats.parseErrors.Load(),
		stats.tokenErrors.Load(),
	)

	if stats.tokens.Load() != int64(*sessions) ||
		stats.joinErrors.Load() > 0 ||
		stats.streamErrors.Load() > 0 ||
		stats.parseErrors.Load() > 0 ||
		stats.tokenErrors.Load() > 0 {
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

func openStreams(
	ctx context.Context,
	client *http.Client,
	baseURL string,
	tenantID string,
	eventID string,
	sessionIDs []string,
	openRate int,
	streamGate <-chan struct{},
	stats *counters,
	totalSessions int64,
	cancel context.CancelFunc,
	wg *sync.WaitGroup,
) {
	interval := time.Second / time.Duration(openRate)
	if interval <= 0 {
		interval = time.Nanosecond
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for _, sessionID := range sessionIDs {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		wg.Add(1)
		go func(sessionID string) {
			defer wg.Done()
			runClientFlow(ctx, client, baseURL, tenantID, eventID, sessionID, streamGate, stats, totalSessions, cancel)
		}(sessionID)
	}
}

func runClientFlow(
	ctx context.Context,
	client *http.Client,
	baseURL string,
	tenantID string,
	eventID string,
	sessionID string,
	streamGate <-chan struct{},
	stats *counters,
	totalSessions int64,
	cancel context.CancelFunc,
) {
	resp, err := openStream(ctx, client, baseURL, tenantID, eventID, sessionID)
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("stream %s failed: %s", sessionID, err)
			stats.streamErrors.Add(1)
		}
		return
	}
	defer resp.Body.Close()

	stats.subscribed.Add(1)
	stats.activeStream.Add(1)
	defer stats.activeStream.Add(-1)

	select {
	case <-streamGate:
	case <-ctx.Done():
		return
	}

	issued, err := readStreamUntilToken(ctx, client, resp.Body, baseURL, tenantID, eventID, sessionID, stats)
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("stream %s failed: %s", sessionID, err)
			stats.streamErrors.Add(1)
		}
		return
	}
	if !issued {
		return
	}

	if stats.tokens.Add(1) == totalSessions {
		cancel()
	}
}

func openStream(ctx context.Context, client *http.Client, baseURL, tenantID, eventID, sessionID string) (*http.Response, error) {
	url := fmt.Sprintf(
		"%s/v1/tenants/%s/events/%s/queue/stream/%s",
		strings.TrimRight(baseURL, "/"),
		tenantID,
		eventID,
		sessionID,
	)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "text/event-stream")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil, fmt.Errorf("stream %s returned %s", sessionID, resp.Status)
	}

	return resp, nil
}

func readStreamUntilToken(
	ctx context.Context,
	client *http.Client,
	body io.Reader,
	baseURL string,
	tenantID string,
	eventID string,
	sessionID string,
	stats *counters,
) (bool, error) {
	scanner := bufio.NewScanner(body)
	var eventType string
	var data strings.Builder

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			issued, err := processSSEEvent(ctx, client, baseURL, tenantID, eventID, sessionID, eventType, data.String(), stats)
			if err != nil {
				return false, err
			}
			if issued {
				return true, nil
			}

			eventType = ""
			data.Reset()
			continue
		}

		if strings.HasPrefix(line, "event:") {
			eventType = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			continue
		}
		if strings.HasPrefix(line, "data:") {
			if data.Len() > 0 {
				data.WriteByte('\n')
			}
			data.WriteString(strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		}
	}

	if err := scanner.Err(); err != nil && ctx.Err() == nil {
		return false, err
	}
	if ctx.Err() != nil {
		return false, nil
	}

	return false, io.ErrUnexpectedEOF
}

func processSSEEvent(
	ctx context.Context,
	client *http.Client,
	baseURL string,
	tenantID string,
	eventID string,
	sessionID string,
	eventType string,
	data string,
	stats *counters,
) (bool, error) {
	if eventType != "queue-status" || data == "" {
		return false, nil
	}

	stats.events.Add(1)

	var status streamStatus
	if err := json.Unmarshal([]byte(data), &status); err != nil {
		stats.parseErrors.Add(1)
		return false, err
	}

	if !status.CanEnter {
		return false, nil
	}

	if err := issueToken(ctx, client, baseURL, tenantID, eventID, sessionID); err != nil {
		log.Printf("token %s failed: %s", sessionID, err)
		stats.tokenErrors.Add(1)
		return false, err
	}

	return true, nil
}

func issueToken(ctx context.Context, client *http.Client, baseURL, tenantID, eventID, sessionID string) error {
	body, err := json.Marshal(map[string]string{"SessionID": sessionID})
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/v1/tenants/%s/events/%s/queue/token", strings.TrimRight(baseURL, "/"), tenantID, eventID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("token %s returned %s: %s", sessionID, resp.Status, strings.TrimSpace(string(responseBody)))
	}

	var token tokenResponse
	if err := json.Unmarshal(responseBody, &token); err != nil {
		return err
	}
	if token.TokenType == "" || token.Token == "" || token.ExpiresIn <= 0 {
		return fmt.Errorf("token %s returned incomplete response", sessionID)
	}

	return nil
}

func waitForSubscriptions(ctx context.Context, stats *counters, totalSessions int64) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		if stats.subscribed.Load()+stats.streamErrors.Load() >= totalSessions {
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
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
					"progress joined=%d subscribed=%d active_streams=%d events=%d tokens=%d join_errors=%d stream_errors=%d parse_errors=%d token_errors=%d\n",
					stats.joined.Load(),
					stats.subscribed.Load(),
					stats.activeStream.Load(),
					stats.events.Load(),
					stats.tokens.Load(),
					stats.joinErrors.Load(),
					stats.streamErrors.Load(),
					stats.parseErrors.Load(),
					stats.tokenErrors.Load(),
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

func envBool(key string, fallback bool) bool {
	value := strings.ToLower(os.Getenv(key))
	switch value {
	case "1", "t", "true", "yes", "y":
		return true
	case "0", "f", "false", "no", "n":
		return false
	case "":
		return fallback
	default:
		return fallback
	}
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
