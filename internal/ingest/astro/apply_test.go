package astro_test

import (
	"testing"
	"time"

	"github.com/inorbit/inorbit/internal/domain"
	"github.com/inorbit/inorbit/internal/engine"
	"github.com/inorbit/inorbit/internal/ingest/astro"
	"github.com/inorbit/inorbit/internal/store"
)

func TestApplyKeepsNewestRunAcrossDeployments(t *testing.T) {
	older := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 9, 17, 0, 0, 0, 0, time.UTC)
	dags := []domain.DAG{
		{DataProductID: "catalog", DAGID: "catalog_daily", DeploymentName: "stage", CompletedAt: &older, Status: "SUCCESS"},
		{DataProductID: "catalog", DAGID: "catalog_daily", DeploymentName: "prod", CompletedAt: &newer, Status: "FAILED"},
		{DataProductID: "catalog", DAGID: "catalog_hourly", DeploymentName: "prod", Status: "SUCCESS"},
	}
	st := store.New()
	st.UpsertProduct(domain.DataProduct{ID: "catalog", Name: "catalog"})
	astro.Apply(st, engine.New(st), dags, newer)
	snap, ok := st.Snapshot("catalog")
	if !ok || len(snap.Pipeline) != 2 {
		t.Fatalf("got %+v ok=%v", snap.Pipeline, ok)
	}
	var daily domain.DAG
	for _, d := range snap.Pipeline {
		if d.DAGID == "catalog_daily" {
			daily = d.DAG
		}
	}
	if daily.DeploymentName != "prod" || daily.Status != "FAILED" {
		t.Fatalf("wanted prod FAILED, got %+v", daily)
	}
}
