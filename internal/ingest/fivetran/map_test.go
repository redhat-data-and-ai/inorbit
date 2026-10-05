package fivetran

import (
	"testing"
	"time"

	"github.com/inorbit/inorbit/internal/domain"
)

func TestConnectionStatusFailedAfterSuccess(t *testing.T) {
	okAt := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	failAt := time.Date(2026, 10, 1, 11, 0, 0, 0, time.UTC)
	st, msg := connectionStatus(connectionAPI{
		Status: connectionStatusAPI{SetupState: "connected", SyncState: "scheduled"},
	}, &okAt, &failAt)
	if st != "FAILED" || msg == "" {
		t.Fatalf("got %s %q", st, msg)
	}
}

func TestConnectionStatusSyncingWithTasksIsFailed(t *testing.T) {
	st, msg := connectionStatus(connectionAPI{
		Status: connectionStatusAPI{
			SetupState: "connected",
			SyncState:  "syncing",
			Tasks:      []statusNote{{Message: "schema missing"}},
		},
	}, nil, nil)
	if st != "FAILED" || msg != "schema missing" {
		t.Fatalf("got %s %q", st, msg)
	}
}

func TestConnectionStatusSyncing(t *testing.T) {
	st, _ := connectionStatus(connectionAPI{
		Status: connectionStatusAPI{SetupState: "connected", SyncState: "syncing"},
	}, nil, nil)
	if st != "RUNNING" {
		t.Fatalf("got %s", st)
	}
}

func TestConnectionStatusBrokenSetup(t *testing.T) {
	st, msg := connectionStatus(connectionAPI{
		Status: connectionStatusAPI{
			SetupState: "broken",
			SyncState:  "scheduled",
			Tasks:      []statusNote{{Message: "auth expired"}},
		},
	}, nil, nil)
	if st != "FAILED" || msg != "auth expired" {
		t.Fatalf("got %s %q", st, msg)
	}
}

func TestToDAGUsesSyncFrequencyAndDashboard(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	okAt := now.Add(-30 * time.Minute)
	p := domain.DataProduct{ID: "beta", Name: "beta", Type: "source-aligned"}
	d := toDAG(p, groupAPI{ID: "g1", Name: "prod"}, connectionAPI{
		ID:            "conn-orders",
		Schema:        "orders",
		Service:       "postgres",
		SucceededAt:   okAt.Format(time.RFC3339),
		SyncFrequency: 60,
		ScheduleType:  "auto",
		Status:        connectionStatusAPI{SetupState: "connected", SyncState: "scheduled"},
	}, "https://fivetran.com/dashboard/connections", now)
	if d.PipelineType != domain.PipelineTypeFivetran || d.Status != "SUCCESS" {
		t.Fatalf("%+v", d)
	}
	if d.IntervalMins == nil || *d.IntervalMins != 60 {
		t.Fatalf("interval %+v", d.IntervalMins)
	}
	if d.SLAMinutes == nil || *d.SLAMinutes != 30 {
		t.Fatalf("sla %+v", d.SLAMinutes)
	}
	if d.AstroURL != "https://fivetran.com/dashboard/connections/conn-orders" {
		t.Fatalf("url %q", d.AstroURL)
	}
	if d.NextExpectedAt == nil || !d.NextExpectedAt.Equal(okAt.Add(time.Hour)) {
		t.Fatalf("next %+v", d.NextExpectedAt)
	}
	if d.TriggerType != "SCHEDULED" {
		t.Fatalf("trigger %q", d.TriggerType)
	}
}

func TestMarkPrimaryPerDestination(t *testing.T) {
	dags := []domain.DAG{
		{DataProductID: "beta", DeploymentName: "prod", DAGID: "beta_extra", PipelineType: domain.PipelineTypeFivetran},
		{DataProductID: "beta", DeploymentName: "prod", DAGID: "beta", PipelineType: domain.PipelineTypeFivetran},
		{DataProductID: "beta", DeploymentName: "stage", DAGID: "beta", PipelineType: domain.PipelineTypeFivetran},
	}
	markPrimary(dags)
	if !dags[1].IsPrimary || !dags[2].IsPrimary || dags[0].IsPrimary {
		t.Fatalf("%+v", dags)
	}
}
