package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

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
	mux.HandleFunc("GET /v1/tenants/{tenantID}/events/{eventID}/queue/status/{sessionID}", s.GetRoomStatus)
	mux.HandleFunc("GET /v1/tenants/{tenantID}/events/{eventID}/queue/stream/{sessionID}", s.GetRoomStream)
	mux.HandleFunc("POST /v1/tenants/{tenantID}/events/{eventID}/queue/token", s.IssueAdmissionToken)
	mux.HandleFunc("POST /v1/tenants/{tenantID}/events/{eventID}/queue/join", s.JoinRoom)
	srv.Handler = mux
	return srv.ListenAndServe()
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
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(response)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func (s HTTPServer) GetRoomStream(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	clientGone := r.Context().Done()

	rc := http.NewResponseController(w)
	t := time.NewTicker(1 * time.Second)

	for {
		select {
		case <-clientGone:
			return
		case <-t.C:
			_, err := fmt.Fprintf(w, "data: The time is %s\n\n", time.Now().Format(time.UnixDate))
			if err != nil {
				return
			}
			err = rc.Flush()
			if err != nil {
				return
			}
		}
	}
}

func (s HTTPServer) IssueAdmissionToken(w http.ResponseWriter, r *http.Request) {
	var request command.IssueAdmissionToken
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	response, err := s.App.Commands.IssueAdmissionToken(r.Context(), request)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	err = json.NewEncoder(w).Encode(response)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func (s HTTPServer) JoinRoom(w http.ResponseWriter, r *http.Request) {
	var request command.JoinRoom
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	response, err := s.App.Commands.JoinRoom(r.Context(), request)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	err = json.NewEncoder(w).Encode(response)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}
