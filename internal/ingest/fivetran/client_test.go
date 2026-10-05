package fivetran

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/inorbit/inorbit/internal/domain"
)

func TestFetchForProductsFromMockAPI(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/groups", func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != "key" || pass != "secret" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": "Success",
			"data": map[string]any{
				"items": []map[string]any{
					{"id": "g-prod", "name": "prod"},
					{"id": "g-stage", "name": "stage"},
				},
			},
		})
	})
	mux.HandleFunc("/groups/g-prod/connections", func(w http.ResponseWriter, _ *http.Request) {
		okAt := time.Now().UTC().Add(-20 * time.Minute).Format(time.RFC3339)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": "Success",
			"data": map[string]any{
				"items": []map[string]any{
					{
						"id": "conn-orders", "schema": "orders", "service": "postgres",
						"paused": false, "succeeded_at": okAt, "sync_frequency": 60,
						"schedule_type": "auto",
						"status":        map[string]any{"setup_state": "connected", "sync_state": "scheduled"},
					},
					{
						"id": "conn-other", "schema": "unrelated", "service": "google_sheets",
						"paused": false, "sync_frequency": 360,
						"status": map[string]any{"setup_state": "connected", "sync_state": "scheduled"},
					},
				},
			},
		})
	})
	mux.HandleFunc("/groups/g-stage/connections", func(w http.ResponseWriter, _ *http.Request) {
		failAt := time.Now().UTC().Add(-5 * time.Minute).Format(time.RFC3339)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": "Success",
			"data": map[string]any{
				"items": []map[string]any{
					{
						"id": "conn-orders-stage", "schema": "orders", "service": "postgres",
						"paused": false, "failed_at": failAt, "sync_frequency": 60,
						"schedule_type": "auto",
						"status": map[string]any{
							"setup_state": "connected", "sync_state": "scheduled",
							"tasks": []map[string]any{{"message": "sync failed"}},
						},
					},
				},
			},
		})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	c := &Client{HTTP: srv.Client(), APIKey: "key", APISecret: "secret", BaseURL: srv.URL}
	got, err := c.FetchForProducts(context.Background(), []domain.DataProduct{
		{ID: "orders", Name: "orders", Type: "source-aligned"},
		{ID: "alpha", Name: "alpha", Type: "aggregate"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("want prod+stage orders, got %+v", got)
	}
	var prod, stage domain.Connector
	for _, d := range got {
		switch d.GroupName {
		case "prod":
			prod = d
		case "stage":
			stage = d
		}
	}
	if prod.Status != "SUCCESS" || prod.Service != "postgres" || prod.Schema != "orders" {
		t.Fatalf("prod %+v", prod)
	}
	if stage.Status != "FAILED" || stage.ErrorMessage != "sync failed" {
		t.Fatalf("stage %+v", stage)
	}
}
