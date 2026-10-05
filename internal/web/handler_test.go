package web_test

import (
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
	"github.com/inorbit/inorbit/internal/ingest/quality"
	"github.com/inorbit/inorbit/internal/store"
	"github.com/inorbit/inorbit/internal/web"
)

func handler(t *testing.T) http.Handler {
	t.Helper()
	st := store.New()
	eng := engine.New(st)
	now := time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)
	products, dags, checks, lineageRows, conns, err := demo.Load(now)
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
	byL := map[string]domain.Lineage{}
	for _, lin := range lineageRows {
		byL[lin.DataProductID] = lin
	}
	byConn := map[string][]domain.Connector{}
	for _, c := range conns {
		byConn[c.DataProductID] = append(byConn[c.DataProductID], c)
	}
	for _, p := range products {
		st.UpsertProduct(p)
		st.SetDAGs(p.ID, byD[p.ID])
		st.SetChecks(p.ID, byC[p.ID])
		st.SetQualitySources(p.ID, quality.ConfigSources(p))
		st.SetLineage(p.ID, byL[p.ID])
		st.SetConnectors(p.ID, byConn[p.ID])
	}
	eng.Recompute(now)
	return web.Mount((&api.Server{Store: st, Alerts: alert.New(st, nil)}).Handler())
}

func TestMountServesConsoleAndLeavesAPI(t *testing.T) {
	h := handler(t)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "InOrbit") {
		t.Fatalf("ui /: %d %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Fatalf("content-type %q", ct)
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/app.js", nil))
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "/v1/snapshots") {
		t.Fatalf("ui js: %d %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "Open in Astro") || !strings.Contains(rec.Body.String(), "d.astro_url") {
		t.Fatal("ui js missing astro_url column")
	}
	if !strings.Contains(rec.Body.String(), "io-src-card") || !strings.Contains(rec.Body.String(), "Data Lineage Overview") {
		t.Fatal("ui js missing lineage source cards")
	}
	if !strings.Contains(rec.Body.String(), "lineageForEnv") || !strings.Contains(rec.Body.String(), "more sources") {
		t.Fatal("ui js missing env-scoped lineage or +N more sources")
	}
	if !strings.Contains(rec.Body.String(), "1 paused") {
		t.Fatal("ui js missing paused chip label")
	}
	if !strings.Contains(rec.Body.String(), "Fivetran connectors") || !strings.Contains(rec.Body.String(), "Open in Fivetran") || !strings.Contains(rec.Body.String(), "c.dashboard_url") {
		t.Fatal("ui js missing Fivetran dashboard links")
	}
	if !strings.Contains(rec.Body.String(), `class="astro-open"`) || !strings.Contains(rec.Body.String(), "astroLink(d, true)") {
		t.Fatal("ui js missing collapsed-row Open in Astro control")
	}
	if strings.Contains(rec.Body.String(), `"freshness", "pipeline"`) || strings.Contains(rec.Body.String(), `?tab=freshness`) {
		t.Fatal("ui still has a separate freshness tab")
	}
	if !strings.Contains(rec.Body.String(), "?tab=pipeline") || !strings.Contains(rec.Body.String(), "Elementary") {
		t.Fatal("ui js missing pipeline tab or Elementary filter")
	}
	if !strings.Contains(rec.Body.String(), "?tab=lineage") || !strings.Contains(rec.Body.String(), "Upstream sources") {
		t.Fatal("ui js missing lineage tab")
	}
	if !strings.Contains(rec.Body.String(), "timeZoneName") || !strings.Contains(rec.Body.String(), "fetch-banner") {
		t.Fatal("ui js missing local timezone fetch banner")
	}
	if !strings.Contains(rec.Body.String(), "Last Run") || !strings.Contains(rec.Body.String(), "Reliability") {
		t.Fatal("ui js missing expandable pipeline columns")
	}
	if !strings.Contains(rec.Body.String(), "flag custom") || !strings.Contains(rec.Body.String(), ">Open</th>") {
		t.Fatal("ui js missing custom badge or Open column")
	}
	if !strings.Contains(rec.Body.String(), "alert-banner") || !strings.Contains(rec.Body.String(), "warnings") {
		t.Fatal("ui js missing ingest warning banner")
	}
	if !strings.Contains(rec.Body.String(), "Validation checks not exist") {
		t.Fatal("ui js missing missing-validation empty state")
	}
	if !strings.Contains(rec.Body.String(), "warehouse pipeline mart") {
		t.Fatal("ui js missing pipeline mart empty hint")
	}
	if !strings.Contains(rec.Body.String(), "io-grid") || !strings.Contains(rec.Body.String(), "io-card") {
		t.Fatal("ui js missing catalog card grid")
	}
	if !strings.Contains(rec.Body.String(), "Search by name") || !strings.Contains(rec.Body.String(), "io-pill") {
		t.Fatal("ui js missing catalog search or type pills")
	}
	if !strings.Contains(rec.Body.String(), "Data Products") || !strings.Contains(rec.Body.String(), "data-csv") {
		t.Fatal("ui js missing catalog title or CSV export")
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("ui js should not be cached by the browser")
	}
	if !strings.Contains(rec.Body.String(), `aria-label="Product sections"`) {
		t.Fatal("ui js missing product tablist")
	}
	if !strings.Contains(rec.Body.String(), "Primary Pipeline") || !strings.Contains(rec.Body.String(), "data-rel-window") {
		t.Fatal("ui js missing primary DAG pipeline and freshness strip")
	}
	if !strings.Contains(rec.Body.String(), "Composite Score across all dimensions") || !strings.Contains(rec.Body.String(), "data-trend-window") {
		t.Fatal("ui js missing overview health scoreboard")
	}
	if !strings.Contains(rec.Body.String(), "io-trend-seg") || !strings.Contains(rec.Body.String(), "warehouse daily health snapshot") {
		t.Fatal("ui js missing warehouse health trend")
	}
	if !strings.Contains(rec.Body.String(), `catalogSelect("product-env"`) || !strings.Contains(rec.Body.String(), `envFilter: "production"`) {
		t.Fatal("ui js missing product environment filter defaulting to production")
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/meta", nil))
	if rec.Code != 200 {
		t.Fatalf("api still mounted: %d %s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "<html") || !strings.Contains(rec.Body.String(), "data_product_count") {
		t.Fatalf("api captured by ui: %s", rec.Body.String())
	}
}
