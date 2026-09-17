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
	"github.com/inorbit/inorbit/internal/store"
	"github.com/inorbit/inorbit/internal/web"
)

func handler(t *testing.T) http.Handler {
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
	if !strings.Contains(rec.Body.String(), `class="astro-open"`) || !strings.Contains(rec.Body.String(), "astroLink(d, true)") {
		t.Fatal("ui js missing collapsed-row Open in Astro control")
	}
	if !strings.Contains(rec.Body.String(), "freshness") || !strings.Contains(rec.Body.String(), "Elementary") {
		t.Fatal("ui js missing freshness tab or Elementary filter")
	}
	if !strings.Contains(rec.Body.String(), "timeZoneName") || !strings.Contains(rec.Body.String(), "fetch-banner") {
		t.Fatal("ui js missing local timezone fetch banner")
	}
	if !strings.Contains(rec.Body.String(), "Last Run") || !strings.Contains(rec.Body.String(), "Reliability") {
		t.Fatal("ui js missing Ludo-style pipeline columns")
	}
	if !strings.Contains(rec.Body.String(), "alert-banner") || !strings.Contains(rec.Body.String(), "warnings") {
		t.Fatal("ui js missing ingest warning banner")
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
