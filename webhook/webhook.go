package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"howett.net/plist"
)

// ---------- 1. plist -> JSON (works for any plist, XML or binary) ----------

func decodePlist(data []byte) (interface{}, error) {
	var v interface{}
	if _, err := plist.Unmarshal(data, &v); err != nil {
		return nil, fmt.Errorf("plist decode: %w", err)
	}
	return v, nil
}

// PlistToJSON converts arbitrary plist bytes into JSON bytes.
// dict -> object, array -> array, data -> base64 string,
// date -> RFC 3339 string, integer/real/bool/string map directly.
func PlistToJSON(data []byte) ([]byte, error) {
	v, err := decodePlist(data)
	if err != nil {
		return nil, err
	}
	out, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("json encode: %w", err)
	}
	return out, nil
}

// ---------- 2. store keyed by CommandUUID, with expiry ----------

type entry struct {
	json       json.RawMessage
	receivedAt time.Time
}

type ResultStore struct {
	mu  sync.RWMutex
	m   map[string]entry
	ttl time.Duration
}

func NewResultStore(ttl time.Duration) *ResultStore {
	return &ResultStore{m: make(map[string]entry), ttl: ttl}
}

func (s *ResultStore) Put(commandUUID string, j []byte) {
	s.mu.Lock()
	s.m[commandUUID] = entry{json: j, receivedAt: time.Now()}
	s.mu.Unlock()
}

// Get returns the stored JSON, treating expired entries as missing
// even if the janitor hasn't swept them yet.
func (s *ResultStore) Get(commandUUID string) (json.RawMessage, bool) {
	s.mu.RLock()
	e, ok := s.m[commandUUID]
	s.mu.RUnlock()
	if !ok || time.Since(e.receivedAt) > s.ttl {
		return nil, false
	}
	return e.json, true
}

func (s *ResultStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.m)
}

// Sweep deletes entries older than the TTL and returns how many it removed.
func (s *ResultStore) Sweep() int {
	cutoff := time.Now().Add(-s.ttl)
	removed := 0
	s.mu.Lock()
	for k, e := range s.m {
		if e.receivedAt.Before(cutoff) {
			delete(s.m, k)
			removed++
		}
	}
	s.mu.Unlock()
	return removed
}

// StartJanitor runs Sweep on a ticker until ctx is cancelled.
func (s *ResultStore) StartJanitor(ctx context.Context, interval time.Duration) {
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if n := s.Sweep(); n > 0 {
					log.Printf("[webhook] swept %d expired results, %d remain", n, s.Len())
				}
			}
		}
	}()
}

// ---------- 3. webhook server ----------

type event struct {
	Topic            string `json:"topic"`
	EventID          string `json:"event_id"`
	AcknowledgeEvent *struct {
		UDID        string `json:"udid"`
		Status      string `json:"status"`
		CommandUUID string `json:"command_uuid"`
		RawPayload  []byte `json:"raw_payload"`
	} `json:"acknowledge_event,omitempty"`
}

func newHandler(store *ResultStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 10<<20)

		var ev event
		if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK) // answer fast; process below

		ack := ev.AcknowledgeEvent
		if ack == nil || ack.Status == "Idle" || len(ack.RawPayload) == 0 {
			return
		}

		v, err := decodePlist(ack.RawPayload)
		if err != nil {
			log.Printf("[webhook] %v", err)
			return
		}

		// Prefer CommandUUID from the plist itself, fall back to the event field.
		uuid := ack.CommandUUID
		if m, ok := v.(map[string]interface{}); ok {
			if id, ok := m["CommandUUID"].(string); ok && id != "" {
				uuid = id
			}
		}
		if uuid == "" {
			return
		}

		j, err := json.Marshal(v)
		if err != nil {
			log.Printf("[webhook] json encode: %v", err)
			return
		}
		store.Put(uuid, j)
		log.Printf("[webhook] udid=%s status=%s command_uuid=%s\n%s", ack.UDID, ack.Status, uuid, j)
	}
}

// Start binds a loopback listener on a free port, serves in the background,
// starts the janitor, and returns the webhook URL plus the result store.
func Start(ctx context.Context) (string, *ResultStore, error) {
	store := NewResultStore(24 * time.Hour)
	store.StartJanitor(ctx, 10*time.Minute)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, fmt.Errorf("webhook: listen: %w", err)
	}
	srv := &http.Server{
		Handler:           newHandler(store),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("[webhook] server stopped: %v", err)
		}
	}()
	go func() { <-ctx.Done(); _ = srv.Close() }()

	return "http://" + ln.Addr().String(), store, nil
}
