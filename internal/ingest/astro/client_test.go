package astro_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/inorbit/inorbit/internal/domain"
	"github.com/inorbit/inorbit/internal/engine"
	"github.com/inorbit/inorbit/internal/ingest/astro"
	"github.com/inorbit/inorbit/internal/store"
)

func TestMatchPrefersLongerProductName(t *testing.T) {
	products := []domain.DataProduct{
		{ID: "orders", Name: "orders"},
		{ID: "ordersmaster", Name: "ordersmaster"},
	}
	got, ok := astro.Match("ordersmaster_daily", nil, products, nil)
	if !ok || got.ID != "ordersmaster" {
		t.Fatalf("got %+v ok=%v", got, ok)
	}
}

func TestMatchUsesTag(t *testing.T) {
	products := []domain.DataProduct{{ID: "catalog", Name: "catalog"}}
	got, ok := astro.Match("warehouse_refresh", []string{"catalog", "is_primary_dag"}, products, nil)
	if !ok || got.ID != "catalog" {
		t.Fatalf("got %+v ok=%v", got, ok)
	}
}

func TestFetchForProductsFromMockAirflow(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/dags", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"total_entries": 6,
			"dags": []map[string]any{
				{
					"dag_id": "catalog_hourly", "is_paused": false,
					"tags": []map[string]any{
						{"name": "catalog"},
						{"name": "is_primary_dag"},
						{"name": "sla:interval_mins:60"},
						{"name": "sla:threshold_mins:30"},
					},
					"next_dagrun": time.Now().UTC().Add(20 * time.Minute).Format(time.RFC3339),
				},
				{
					"dag_id": "ordersmaster_daily", "is_paused": false,
					"tags": []map[string]any{
						{"name": "ordersmaster"},
						{"name": "is_primary_dag"},
						{"name": "sla:interval_mins:1440"},
					},
					"next_dagrun": time.Now().UTC().Add(12 * time.Hour).Format(time.RFC3339),
				},
				{
					"dag_id": "catalog_never_ran", "is_paused": false,
					"tags": []map[string]any{{"name": "catalog"}},
				},
				{
					"dag_id": "legacy_load", "is_paused": true, "is_stale": true,
					"tags": []string{"catalog", "is_custom_dag"},
				},
				{
					"dag_id": "untagged_custom", "is_paused": true, "tags": nil,
				},
				{"dag_id": "unrelated_other", "is_paused": true, "tags": []map[string]any{}},
			},
		})
	})
	mux.HandleFunc("/dags/catalog_hourly/dagRuns", func(w http.ResponseWriter, r *http.Request) {
		end := time.Now().UTC().Add(-10 * time.Minute)
		start := end.Add(-8 * time.Minute)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"dag_runs": []map[string]any{{
				"dag_run_id": "manual__catalog", "state": "success",
				"start_date": start.Format(time.RFC3339),
				"end_date":   end.Format(time.RFC3339),
			}},
		})
	})
	mux.HandleFunc("/dags/untagged_custom", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"dag_id": "untagged_custom", "is_paused": true,
			"tags": []map[string]any{{"name": "catalog"}, {"name": "is_custom_dag"}},
		})
	})
	mux.HandleFunc("/dags/untagged_custom/dagRuns", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"dag_runs": []map[string]any{}})
	})
	mux.HandleFunc("/dags/catalog_never_ran/dagRuns", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"dag_runs": []map[string]any{}})
	})
	mux.HandleFunc("/dags/legacy_load/dagRuns", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"dag_runs": []map[string]any{}})
	})
	mux.HandleFunc("/dags/ordersmaster_daily/dagRuns", func(w http.ResponseWriter, r *http.Request) {
		end := time.Now().UTC().Add(-2 * time.Hour)
		start := end.Add(-25 * time.Minute)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"dag_runs": []map[string]any{{
				"dag_run_id": "scheduled__orders", "state": "success",
				"start_date": start.Format(time.RFC3339),
				"end_date":   end.Format(time.RFC3339),
			}},
		})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	client := &astro.Client{
		HTTP: srv.Client(),
		Deployments: []astro.Deployment{{
			Name: "prod", ID: "prod", AirflowAPIURL: srv.URL,
		}},
	}
	products := []domain.DataProduct{
		{ID: "catalog", Name: "catalog"},
		{ID: "ordersmaster", Name: "ordersmaster"},
	}
	dags, err := client.FetchForProducts(context.Background(), products, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(dags) != 3 {
		t.Fatalf("want 3 matched DAGs, got %d", len(dags))
	}

	st := store.New()
	for _, p := range products {
		st.UpsertProduct(p)
	}
	eng := engine.New(st)
	astro.Apply(st, eng, dags, time.Now().UTC())

	snap, ok := st.Snapshot("catalog")
	if !ok {
		t.Fatal("missing catalog snapshot")
	}
	if len(snap.Pipeline) != 2 {
		t.Fatalf("catalog pipeline want 2 DAGs, got %+v", snap.Pipeline)
	}
	var hourlyOK bool
	var customPaused int
	for _, d := range snap.Pipeline {
		if d.DAGID == "catalog_hourly" && d.Status == "SUCCESS" && d.IsPrimary {
			hourlyOK = true
		}
		if d.DAGID == "untagged_custom" && d.IsPaused && d.IsCustom {
			customPaused++
		}
	}
	if !hourlyOK || customPaused != 1 {
		t.Fatalf("catalog pipeline: %+v", snap.Pipeline)
	}
	wantURL := srv.URL + "/prod/dags/catalog_hourly"
	foundURL := false
	for _, d := range snap.Pipeline {
		if d.DAGID == "catalog_hourly" {
			if d.AstroURL != wantURL {
				t.Fatalf("astro_url %q want %q", d.AstroURL, wantURL)
			}
			foundURL = true
		}
	}
	if !foundURL {
		t.Fatal("missing catalog_hourly astro_url")
	}
	for _, d := range snap.Pipeline {
		if d.DAGID == "catalog_hourly" {
			if d.TriggerType != "MANUAL" || d.FrequencyDisplay != "Every 1 hour" {
				t.Fatalf("hourly display %+v", d.DAG)
			}
			if d.Reliability7d == nil || *d.Reliability7d != 100 {
				t.Fatalf("hourly reliability %+v", d.DAG)
			}
		}
	}
	if !strings.EqualFold(string(snap.Freshness.FreshnessStatus), "GREEN") && snap.Freshness.FreshnessStatus != domain.FreshnessYellow {
		if snap.Freshness.FreshnessStatus != domain.FreshnessGreen {
			t.Fatalf("catalog freshness %s", snap.Freshness.FreshnessStatus)
		}
	}

	snap, ok = st.Snapshot("ordersmaster")
	if !ok || len(snap.Pipeline) != 1 {
		t.Fatalf("ordersmaster snapshot ok=%v pipe=%d", ok, len(snap.Pipeline))
	}
}
