package engine_test

import (
	"strings"
	"testing"
	"time"

	"github.com/inorbit/inorbit/internal/domain"
	"github.com/inorbit/inorbit/internal/engine"
	"github.com/inorbit/inorbit/internal/store"
)

func TestFivetranFailureScoresLikePipeline(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	failAt := now.Add(-10 * time.Minute)
	interval := 60.0
	sla := 30.0
	st := store.New()
	st.UpsertProduct(domain.DataProduct{ID: "beta", Name: "beta", Type: "source-aligned"})
	st.SetConnectors("beta", []domain.Connector{{
		DataProductID: "beta",
		ID:            "conn-orders",
		Schema:        "orders",
		Service:       "postgres",
		GroupName:     "prod",
		Status:        "FAILED",
		CompletedAt:   &failAt,
		FailedAt:      &failAt,
		IntervalMins:  &interval,
		SLAMinutes:    &sla,
	}})
	engine.New(st).Recompute(now)
	snap, ok := st.Snapshot("beta")
	if !ok {
		t.Fatal("missing snapshot")
	}
	var sawPipe bool
	for _, c := range snap.Quality {
		if c.SourceType == domain.SrcFivetranPipeline && c.Status == domain.CheckFailed {
			sawPipe = true
		}
		if strings.Contains(c.Name, "Astro") {
			t.Fatalf("unexpected Astro check %+v", c)
		}
	}
	if !sawPipe {
		t.Fatalf("expected Fivetran sync check, quality=%+v", snap.Quality)
	}
	if snap.Health.FivetranCheckCount == 0 {
		t.Fatalf("fivetran_check_count %+v", snap.Health)
	}
	if snap.Health.Status != domain.HealthAtRisk || snap.Health.HealthScore >= 80 {
		t.Fatalf("failed Fivetran should deduct like a failed pipeline, health=%+v", snap.Health)
	}
	if len(snap.Pipeline) != 0 {
		t.Fatalf("connectors should not appear as pipeline rows %+v", snap.Pipeline)
	}
	if len(snap.Connectors) != 1 {
		t.Fatalf("connectors %+v", snap.Connectors)
	}
}

func TestFivetranStageDoesNotScoreWhenProdExists(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	okAt := now.Add(-10 * time.Minute)
	failAt := now.Add(-5 * time.Minute)
	interval := 60.0
	sla := 30.0
	next := okAt.Add(time.Hour)
	st := store.New()
	st.UpsertProduct(domain.DataProduct{ID: "beta", Name: "beta", Type: "source-aligned"})
	st.SetConnectors("beta", []domain.Connector{
		{
			DataProductID: "beta", ID: "conn-prod", Schema: "orders", Service: "postgres",
			GroupName: "prod", Status: "SUCCESS", CompletedAt: &okAt, SucceededAt: &okAt,
			IntervalMins: &interval, SLAMinutes: &sla, NextExpectedAt: &next,
		},
		{
			DataProductID: "beta", ID: "conn-stage", Schema: "orders", Service: "postgres",
			GroupName: "stage", Status: "FAILED", CompletedAt: &failAt, FailedAt: &failAt,
			IntervalMins: &interval, SLAMinutes: &sla,
		},
	})
	engine.New(st).Recompute(now)
	snap, _ := st.Snapshot("beta")
	for _, c := range snap.Quality {
		if strings.Contains(c.ID, "stage") {
			t.Fatalf("stage fivetran should not score when prod exists: %+v", c)
		}
	}
}

func TestSourceAlignedLineageGroupsConnectors(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	st := store.New()
	st.UpsertProduct(domain.DataProduct{ID: "beta", Name: "beta", Type: "source-aligned"})
	st.SetLineage("beta", domain.Lineage{
		UpstreamSources: []domain.LineageNode{
			{Name: "SNOWPIPE_DB.RAW", Type: "snowpipe_db"},
			{Name: "FIVETRAN_DB.ORDERS", Type: "fivetran_db"},
		},
	})
	st.SetConnectors("beta", []domain.Connector{
		{DataProductID: "beta", Schema: "orders", Service: "google_sheets", Status: "SUCCESS", Paused: true, GroupName: "prod"},
		{DataProductID: "beta", Schema: "orders", Service: "google_sheets", Status: "SUCCESS", GroupName: "prod"},
		{DataProductID: "beta", Schema: "orders", Service: "postgres", Status: "SUCCESS", GroupName: "prod"},
	})
	engine.New(st).Recompute(now)
	snap, _ := st.Snapshot("beta")
	if len(snap.Pipeline) != 0 {
		t.Fatalf("pipeline %+v", snap.Pipeline)
	}
	var snow, sheets, pg int
	for _, n := range snap.Lineage.UpstreamSources {
		if n.Type == "snowpipe_db" {
			snow++
			if n.ConnectorType != "" {
				t.Fatalf("snowpipe %+v", n)
			}
		}
		if n.ConnectorType == "Google Sheets" && n.ConnectionCount == 2 && n.PausedCount == 1 {
			sheets++
		}
		if n.ConnectorType == "Postgres" && n.ConnectionCount == 1 && n.PausedCount == 0 {
			pg++
		}
	}
	if snow != 1 || sheets != 1 || pg != 1 {
		t.Fatalf("lineage %+v", snap.Lineage.UpstreamSources)
	}
}

func TestMartHealthFillsWhenNoLiveChecks(t *testing.T) {
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	st := store.New()
	st.UpsertProduct(domain.DataProduct{ID: "beta", Name: "beta", Type: "source-aligned"})
	st.SetMartHealth(map[string][]domain.HealthPoint{
		"beta": {{At: now.AddDate(0, 0, -1), Score: 100, Status: domain.HealthTrusted, TotalChecks: 12}},
	})
	engine.New(st).Recompute(now)
	snap, ok := st.Snapshot("beta")
	if !ok {
		t.Fatal("missing snapshot")
	}
	if snap.Health.Status != domain.HealthTrusted || snap.Health.HealthScore != 100 {
		t.Fatalf("expected warehouse snapshot fallback %+v", snap.Health)
	}
	hist := st.HealthHistory("beta")
	if len(hist) < 1 || hist[len(hist)-1].Score != 100 {
		t.Fatalf("trend %+v", hist)
	}
}

func TestLiveChecksWinOverMartHealth(t *testing.T) {
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	failAt := now.Add(-10 * time.Minute)
	interval := 60.0
	sla := 30.0
	st := store.New()
	st.UpsertProduct(domain.DataProduct{ID: "beta", Name: "beta", Type: "source-aligned"})
	st.SetMartHealth(map[string][]domain.HealthPoint{
		"beta": {{At: now.AddDate(0, 0, -1), Score: 100, Status: domain.HealthTrusted, TotalChecks: 8}},
	})
	st.SetConnectors("beta", []domain.Connector{{
		DataProductID: "beta", ID: "c1", Schema: "orders", Service: "postgres",
		GroupName: "prod", Status: "FAILED", FailedAt: &failAt, CompletedAt: &failAt,
		IntervalMins: &interval, SLAMinutes: &sla,
	}})
	engine.New(st).Recompute(now)
	snap, _ := st.Snapshot("beta")
	if snap.Health.HealthScore >= 80 || snap.Health.Status != domain.HealthAtRisk {
		t.Fatalf("live Fivetran should win over warehouse 100 %+v", snap.Health)
	}
}
