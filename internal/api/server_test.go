package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/inorbit/inorbit/internal/alert"
	"github.com/inorbit/inorbit/internal/api"
	"github.com/inorbit/inorbit/internal/demo"
	"github.com/inorbit/inorbit/internal/domain"
	"github.com/inorbit/inorbit/internal/engine"
	"github.com/inorbit/inorbit/internal/store"
)

func setup(t *testing.T) (http.Handler, []domain.DataProduct, []domain.DAG) {
	t.Helper()
	st := store.New()
	eng := engine.New(st)
	now := time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)
	products, dags, checks, err := demo.Load(now)
	if err != nil {
		t.Fatal(err)
	}
	byD := map[string][]domain.DAG{}
	for _, d := range dags {
		byD[d.DataProductID] = append(byD[d.DataProductID], d)
	}
	byC := map[string][]domain.Check{}
	for _, c := range checks {
		byC[c.DataProductID] = append(byC[c.DataProductID], c)
	}
	for _, p := range products {
		st.UpsertProduct(p)
		st.SetDAGs(p.ID, byD[p.ID])
		st.SetChecks(p.ID, byC[p.ID])
	}
	eng.Recompute(now)
	return (&api.Server{Store: st, Alerts: alert.New(st, nil)}).Handler(), products, dags
}

func TestSnapshotsAndDemoFreshness(t *testing.T) {
	h, products, dags := setup(t)
	if len(products) != 2 {
		t.Fatalf("want 2 DPs from demo catalog, got %d", len(products))
	}
	atRisk, healthy := products[0], products[1]

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/snapshots", nil))
	if rec.Code != 200 {
		t.Fatalf("status %d %s", rec.Code, rec.Body.String())
	}
	var snaps []domain.Snapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &snaps); err != nil {
		t.Fatal(err)
	}
	if len(snaps) != 2 {
		t.Fatalf("want 2 DPs, got %d", len(snaps))
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/data-products/"+atRisk.ID+"/freshness", nil))
	var fr domain.FreshnessSLA
	_ = json.Unmarshal(rec.Body.Bytes(), &fr)
	if fr.FreshnessStatus != domain.FreshnessRed {
		t.Fatalf("%s should be RED in demo catalog, got %s", atRisk.ID, fr.FreshnessStatus)
	}

	rec = httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/subscriptions", strings.NewReader(
		`{"data_product_id":"`+atRisk.ID+`","audience":"developer","slack_channel":"`+atRisk.SlackChannel+`"}`,
	))
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("subscribe %d %s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/data-products/"+atRisk.ID+"/health-trend", nil))
	if rec.Code != 200 {
		t.Fatalf("health-trend %d %s", rec.Code, rec.Body.String())
	}
	var pts []domain.HealthPoint
	if err := json.Unmarshal(rec.Body.Bytes(), &pts); err != nil {
		t.Fatal(err)
	}
	if len(pts) < 1 {
		t.Fatal("expected at least one health trend sample")
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/meta", nil))
	if rec.Code != 200 {
		t.Fatalf("meta %d %s", rec.Code, rec.Body.String())
	}
	var meta domain.PollMeta
	if err := json.Unmarshal(rec.Body.Bytes(), &meta); err != nil {
		t.Fatal(err)
	}
	if meta.Scale.TargetDataProducts != domain.ScaleTargetDataProducts || meta.Scale.EstimatedRSSBytesAtTarget <= 0 {
		t.Fatalf("scale %+v", meta.Scale)
	}

	var atRiskDAG, healthyDAG domain.DAG
	for _, d := range dags {
		if d.DataProductID == atRisk.ID && d.IsPrimary {
			atRiskDAG = d
		}
		if d.DataProductID == healthy.ID && d.IsPrimary {
			healthyDAG = d
		}
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/data-products/"+atRisk.ID+"/pipeline", nil))
	if rec.Code != 200 {
		t.Fatalf("pipeline %d %s", rec.Code, rec.Body.String())
	}
	var pipe []domain.PipelineStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &pipe); err != nil {
		t.Fatal(err)
	}
	if len(pipe) == 0 || pipe[0].AstroURL != atRiskDAG.AstroURL {
		t.Fatalf("astro_url %+v want %q", pipe, atRiskDAG.AstroURL)
	}
	if pipe[0].FrequencyDisplay != "Every 1 hour" || pipe[0].PipelineType != "DAG" {
		t.Fatalf("display %+v", pipe[0])
	}
	var sawCustom bool
	for _, row := range pipe {
		if row.IsCustom {
			sawCustom = true
		}
	}
	if !sawCustom {
		t.Fatal("demo alpha should include a custom DAG")
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/data-products/"+healthy.ID+"/pipeline", nil))
	if err := json.Unmarshal(rec.Body.Bytes(), &pipe); err != nil {
		t.Fatal(err)
	}
	if len(pipe) == 0 || pipe[0].FrequencyDisplay != "Every 1 day" || pipe[0].Reliability7d == nil || *pipe[0].Reliability7d != 100 {
		t.Fatalf("%s pipeline %+v", healthy.ID, pipe)
	}
	if pipe[0].AstroURL != healthyDAG.AstroURL {
		t.Fatalf("healthy astro_url %q want %q", pipe[0].AstroURL, healthyDAG.AstroURL)
	}
}
