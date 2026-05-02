package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type backendContextKey struct{}

type backendPool struct {
	host string
	port string

	next atomic.Uint64

	mu        sync.RWMutex
	backends  []*url.URL
	signature string
}

func main() {
	addr := env("LOAD_BALANCER_ADDR", ":8080")
	pool := &backendPool{
		host: env("LOAD_BALANCER_BACKEND_HOST", "queue"),
		port: env("LOAD_BALANCER_BACKEND_PORT", "8080"),
	}

	refreshInterval := envDuration("LOAD_BALANCER_REFRESH_INTERVAL", time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := pool.refresh(ctx); err != nil {
		log.Printf("resolve queue backends: %s", err)
	}
	go pool.refreshLoop(ctx, refreshInterval)

	proxy := &httputil.ReverseProxy{
		Director: func(r *http.Request) {
			target, _ := r.Context().Value(backendContextKey{}).(*url.URL)
			if target == nil {
				return
			}
			r.URL.Scheme = target.Scheme
			r.URL.Host = target.Host
			r.Host = target.Host
		},
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:        20000,
			MaxIdleConnsPerHost: 20000,
			IdleConnTimeout:     90 * time.Second,
		},
		FlushInterval: -1,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			http.Error(w, err.Error(), http.StatusBadGateway)
		},
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		target := pool.pick()
		if target == nil {
			refreshCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			err := pool.refresh(refreshCtx)
			cancel()
			target = pool.pick()
			if target == nil {
				if err != nil {
					log.Printf("resolve queue backends: %s", err)
				}
				http.Error(w, "no queue backends available", http.StatusServiceUnavailable)
				return
			}
		}

		proxy.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), backendContextKey{}, target)))
	})

	log.Printf("load balancer started addr=%s backend=%s:%s", addr, pool.host, pool.port)
	err := http.ListenAndServe(addr, handler)
	if err != nil {
		log.Fatal(err)
	}
}

func (p *backendPool) refreshLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := p.refresh(ctx); err != nil {
				log.Printf("resolve queue backends: %s", err)
			}
		}
	}
}

func (p *backendPool) refresh(ctx context.Context) error {
	hosts, err := net.DefaultResolver.LookupHost(ctx, p.host)
	if err != nil {
		return err
	}
	sort.Strings(hosts)

	backends := make([]*url.URL, 0, len(hosts))
	for _, host := range hosts {
		backends = append(backends, &url.URL{
			Scheme: "http",
			Host:   net.JoinHostPort(host, p.port),
		})
	}

	signature := strconv.Itoa(len(backends))
	for _, backend := range backends {
		signature += "|" + backend.String()
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.backends = backends
	if p.signature != signature {
		p.signature = signature
		log.Printf("queue backends: %s", signature)
	}

	return nil
}

func (p *backendPool) pick() *url.URL {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.backends) == 0 {
		return nil
	}

	index := p.next.Add(1) - 1
	return p.backends[int(index%uint64(len(p.backends)))]
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
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
