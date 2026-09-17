package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/inorbit/inorbit/internal/alert"
	"github.com/inorbit/inorbit/internal/domain"
	"github.com/inorbit/inorbit/internal/store"
)

type Server struct {
	Store  *store.Memory
	Alerts *alert.Router
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /v1/meta", s.meta)
	mux.HandleFunc("GET /v1/snapshots", s.snapshots)
	mux.HandleFunc("GET /v1/data-products/{id}", s.dp)
	mux.HandleFunc("GET /v1/data-products/{id}/health", s.health)
	mux.HandleFunc("GET /v1/data-products/{id}/pipeline", s.pipeline)
	mux.HandleFunc("GET /v1/data-products/{id}/freshness", s.freshness)
	mux.HandleFunc("GET /v1/data-products/{id}/quality", s.quality)
	mux.HandleFunc("GET /v1/data-products/{id}/health-trend", s.healthTrend)
	mux.HandleFunc("GET /v1/subscriptions", s.listSubs)
	mux.HandleFunc("POST /v1/subscriptions", s.createSub)
	mux.HandleFunc("DELETE /v1/subscriptions/{id}", s.deleteSub)
	return cors(mux)
}

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "inorbit"})
}

func (s *Server) meta(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.Store.Meta())
}

func (s *Server) snapshots(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.Store.AllSnapshots())
}

func (s *Server) snap(w http.ResponseWriter, r *http.Request) (domain.Snapshot, bool) {
	id := r.PathValue("id")
	snap, ok := s.Store.Snapshot(id)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "data product not found", "id": id})
		return domain.Snapshot{}, false
	}
	return snap, true
}

func (s *Server) dp(w http.ResponseWriter, r *http.Request) {
	snap, ok := s.snap(w, r)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	snap, ok := s.snap(w, r)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, snap.Health)
}

func (s *Server) pipeline(w http.ResponseWriter, r *http.Request) {
	snap, ok := s.snap(w, r)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, snap.Pipeline)
}

func (s *Server) freshness(w http.ResponseWriter, r *http.Request) {
	snap, ok := s.snap(w, r)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, snap.Freshness)
}

func (s *Server) healthTrend(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, ok := s.Store.Snapshot(id); !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "data product not found", "id": id})
		return
	}
	writeJSON(w, http.StatusOK, s.Store.HealthHistory(id))
}

func (s *Server) quality(w http.ResponseWriter, r *http.Request) {
	snap, ok := s.snap(w, r)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, qualitySignals(snap.Quality))
}

func (s *Server) listSubs(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.Store.Subs())
}

type subReq struct {
	DataProductID string `json:"data_product_id"`
	Audience      string `json:"audience"`
	SlackChannel  string `json:"slack_channel"`
	CreatedBy     string `json:"created_by"`
}

func (s *Server) createSub(w http.ResponseWriter, r *http.Request) {
	var req subReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if req.DataProductID == "" || req.SlackChannel == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "data_product_id and slack_channel required"})
		return
	}
	aud := domain.Audience(strings.ToLower(req.Audience))
	if aud != domain.AudienceBusiness {
		aud = domain.AudienceDeveloper
	}
	sub := domain.Subscription{
		ID:            fmtID(req.DataProductID, string(aud), req.SlackChannel),
		DataProductID: req.DataProductID,
		Audience:      aud,
		SlackChannel:  req.SlackChannel,
		CreatedBy:     req.CreatedBy,
		CreatedAt:     time.Now().UTC(),
	}
	s.Store.AddSub(sub)
	writeJSON(w, http.StatusCreated, sub)
}

func (s *Server) deleteSub(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !s.Store.DeleteSub(id) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "subscription not found"})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func qualitySignals(checks []domain.Check) []domain.Check {
	out := make([]domain.Check, 0, len(checks))
	for _, c := range checks {
		switch c.SourceType {
		case domain.SrcValidation, domain.SrcDBTTest:
			out = append(out, c)
		}
	}
	return out
}

func fmtID(parts ...string) string {
	return strings.Join(parts, ":")
}
