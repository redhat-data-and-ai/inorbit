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

func TestMergeLiveAndMartPrefersLive(t *testing.T) {
	live := []domain.DAG{{DataProductID: "alpha", DAGID: "alpha_hourly", Status: "RUNNING", DeploymentName: "prod"}}
	mart := []domain.DAG{
		{DataProductID: "alpha", DAGID: "alpha_hourly", Status: "SUCCESS", DeploymentName: "prod"},
		{DataProductID: "gamma", DAGID: "gamma_daily", Status: "SUCCESS", DeploymentName: "extra-prod", AstroURL: "https://astro.example.com/dep/dags/gamma_daily"},
	}
	got := astro.MergeLiveAndMart(live, mart)
	if len(got) != 2 {
		t.Fatalf("len %d %+v", len(got), got)
	}
	if got[0].Status != "RUNNING" || got[0].DAGID != "alpha_hourly" {
		t.Fatalf("live should win %+v", got[0])
	}
	if got[1].DAGID != "gamma_daily" || got[1].AstroURL == "" {
		t.Fatalf("mart fill %+v", got[1])
	}
}

func TestMapMartMatchesCatalogProduct(t *testing.T) {
	rows := []map[string]any{
		{
			"DATA_PRODUCT_ID":            "gamma",
			"DATA_PRODUCT_NAME":          "gamma",
			"DAG_ID":                     "gamma_daily",
			"DAG_STATUS":                 "success",
			"ASTRO_DEPLOYMENT_NAME":      "extra prod",
			"ASTRO_URL":                  "https://astro.example.com/abc/dags/gamma_daily",
			"DAG_IS_PAUSED":              false,
			"IS_PRIMARY_DAG":             true,
			"DAG_EXPECTED_INTERVAL_MINS": 1440.0,
		},
		{"DATA_PRODUCT_ID": "other", "DAG_ID": "dbt_other_daily", "DAG_STATUS": "SUCCESS"},
	}
	got := astro.MapMart(rows, []domain.DataProduct{{ID: "gamma", Name: "gamma"}})
	if len(got) != 1 || got[0].DAGID != "gamma_daily" || got[0].Status != "SUCCESS" {
		t.Fatalf("%+v", got)
	}
	if got[0].AstroURL != "https://astro.example.com/abc/dags/gamma_daily" {
		t.Fatalf("astro_url %q", got[0].AstroURL)
	}
}
