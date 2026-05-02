package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/leandersteiner/go-waiting-room/internal/waitingroom"
	"github.com/leandersteiner/go-waiting-room/internal/waitingroom/app"
	"github.com/leandersteiner/go-waiting-room/internal/waitingroom/app/command"
	"github.com/leandersteiner/go-waiting-room/internal/waitingroom/app/query"
)

type HTTPServer struct {
	App *app.App
}

func NewHTTPServer(app *app.App) *HTTPServer {
	return &HTTPServer{App: app}
}

func (s HTTPServer) Run(srv *http.Server) error {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /.well-known/jwks.json", s.GetAdmissionTokenJWKSet)
	mux.HandleFunc("GET /v1/tenants/{tenantID}/events/{eventID}/queue/status/{sessionID}", s.GetRoomStatus)
	mux.HandleFunc("GET /v1/tenants/{tenantID}/events/{eventID}/queue/stream/{sessionID}", s.GetRoomStream)
	mux.HandleFunc("POST /v1/tenants/{tenantID}/events/{eventID}/queue/token", s.IssueAdmissionToken)
	mux.HandleFunc("POST /v1/tenants/{tenantID}/events/{eventID}/queue/admission/release", s.ReleaseAdmission)
	mux.HandleFunc("POST /v1/tenants/{tenantID}/events/{eventID}/queue/join", s.JoinRoom)
	srv.Handler = mux
	return srv.ListenAndServe()
}

func (s HTTPServer) GetAdmissionTokenJWKSet(w http.ResponseWriter, r *http.Request) {
	if s.App.AdmissionTokenKeys == nil {
		http.Error(w, "admission token keys are not available", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(s.App.AdmissionTokenKeys.AdmissionTokenJWKSet()); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s HTTPServer) GetRoomStatus(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenantID")
	if strings.TrimSpace(tenantID) == "" {
		http.Error(w, "tenantID is required", http.StatusBadRequest)
		return
	}
	eventID := r.PathValue("eventID")
	if strings.TrimSpace(eventID) == "" {
		http.Error(w, "eventID is required", http.StatusBadRequest)
		return
	}
	sessionID := r.PathValue("sessionID")
	if strings.TrimSpace(sessionID) == "" {
		http.Error(w, "sessionID is required", http.StatusBadRequest)
		return
	}

	response, err := s.App.Queries.RoomStatus(r.Context(), query.RoomStatus{
		TenantID:  tenantID,
		EventID:   eventID,
		SessionID: sessionID,
	})
	if err != nil {
		writeError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s HTTPServer) GetRoomStream(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenantID")
	if strings.TrimSpace(tenantID) == "" {
		http.Error(w, "tenantID is required", http.StatusBadRequest)
		return
	}
	eventID := r.PathValue("eventID")
	if strings.TrimSpace(eventID) == "" {
		http.Error(w, "eventID is required", http.StatusBadRequest)
		return
	}
	sessionID := r.PathValue("sessionID")
	if strings.TrimSpace(sessionID) == "" {
		http.Error(w, "sessionID is required", http.StatusBadRequest)
		return
	}

	rc := http.NewResponseController(w)
	// SSE responses are long-lived; the server-wide WriteTimeout is too short for this endpoint.
	_ = rc.SetWriteDeadline(time.Time{})

	started := false

	err := s.App.Queries.StreamRoomStatus(r.Context(), query.StreamRoomStatus{
		TenantID:       tenantID,
		EventID:        eventID,
		SessionID:      sessionID,
		UpdateInterval: 5 * time.Second,
	}, func(response query.StreamRoomStatusResponse) error {
		if !started {
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Cache-Control", "no-cache")
			w.Header().Set("Connection", "keep-alive")
			w.WriteHeader(http.StatusOK)
			started = true
		}

		payload, err := json.Marshal(response)
		if err != nil {
			return err
		}

		_, err = fmt.Fprintf(w, "event: queue-status\ndata: %s\n\n", payload)
		if err != nil {
			return err
		}

		return rc.Flush()
	})
	if err != nil && !started {
		writeError(w, err)
	}
}

func (s HTTPServer) IssueAdmissionToken(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenantID")
	if strings.TrimSpace(tenantID) == "" {
		http.Error(w, "tenantID is required", http.StatusBadRequest)
		return
	}
	eventID := r.PathValue("eventID")
	if strings.TrimSpace(eventID) == "" {
		http.Error(w, "eventID is required", http.StatusBadRequest)
		return
	}

	var request command.IssueAdmissionToken
	if err := decodeJSONBody(r, &request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	request.TenantID = tenantID
	request.EventID = eventID

	response, err := s.App.Commands.IssueAdmissionToken(r.Context(), request)
	if err != nil {
		writeError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	err = json.NewEncoder(w).Encode(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s HTTPServer) ReleaseAdmission(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenantID")
	if strings.TrimSpace(tenantID) == "" {
		http.Error(w, "tenantID is required", http.StatusBadRequest)
		return
	}
	eventID := r.PathValue("eventID")
	if strings.TrimSpace(eventID) == "" {
		http.Error(w, "eventID is required", http.StatusBadRequest)
		return
	}

	var request command.ReleaseAdmission
	if err := decodeJSONBody(r, &request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	request.TenantID = tenantID
	request.EventID = eventID

	response, err := s.App.Commands.ReleaseAdmission(r.Context(), request)
	if err != nil {
		writeError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s HTTPServer) JoinRoom(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenantID")
	if strings.TrimSpace(tenantID) == "" {
		http.Error(w, "tenantID is required", http.StatusBadRequest)
		return
	}
	eventID := r.PathValue("eventID")
	if strings.TrimSpace(eventID) == "" {
		http.Error(w, "eventID is required", http.StatusBadRequest)
		return
	}

	var request command.JoinRoom
	if err := decodeJSONBody(r, &request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	request.TenantID = tenantID
	request.EventID = eventID

	response, err := s.App.Commands.JoinRoom(r.Context(), request)
	if err != nil {
		writeError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	err = json.NewEncoder(w).Encode(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func decodeJSONBody(r *http.Request, destination any) error {
	err := json.NewDecoder(r.Body).Decode(destination)
	if errors.Is(err, io.EOF) {
		return nil
	}

	return err
}

func writeError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, command.ErrInvalidReleaseAdmission):
		http.Error(w, err.Error(), http.StatusBadRequest)
	case errors.Is(err, waitingroom.ErrRoomNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
	case errors.Is(err, waitingroom.ErrSessionNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
	case errors.Is(err, waitingroom.ErrSessionNotAdmitted):
		http.Error(w, err.Error(), http.StatusConflict)
	case errors.Is(err, waitingroom.ErrAdmissionCapacityFull):
		http.Error(w, err.Error(), http.StatusConflict)
	case errors.Is(err, waitingroom.ErrAdmissionLeaseMismatch):
		http.Error(w, err.Error(), http.StatusConflict)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
