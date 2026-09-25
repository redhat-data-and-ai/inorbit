package pipeline_test

import (
	"testing"
	"time"

	"github.com/inorbit/inorbit/internal/domain"
	"github.com/inorbit/inorbit/internal/pipeline"
)

func f64(v float64) *float64 { return &v }
func ts(t time.Time) *time.Time { return &t }

func TestSLAClockFreshnessRedWithoutNewAstroCall(t *testing.T) {
	now := time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)
	last := now.Add(-95 * time.Minute)
	next := last.Add(60 * time.Minute)
	dag := domain.DAG{
		DAGID: "catalog_hourly", Status: "SUCCESS",
		CompletedAt: ts(last), LastSuccessAt: ts(last),
		IntervalMins: f64(60), SLAMinutes: f64(30), NextExpectedAt: ts(next),
		IsPrimary: true, SilentMonitored: true,
	}
	ev := pipeline.Tick(dag, now)
	if ev.FreshnessBand != domain.FreshnessRed {
		t.Fatalf("expected RED, got %s desc=%s", ev.FreshnessBand, ev.Description)
	}
	if ev.Overall != domain.OverallAtRisk {
		t.Fatalf("expected AT_RISK overall, got %s", ev.Overall)
	}
}

func TestSLAClockGreenWhenNextExpectedInFuture(t *testing.T) {
	now := time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)
	last := now.Add(-2 * time.Hour)
	next := last.Add(24 * time.Hour)
	dag := domain.DAG{
		Status: "SUCCESS", CompletedAt: ts(last),
		IntervalMins: f64(1440), SLAMinutes: f64(180), NextExpectedAt: ts(next),
	}
	ev := pipeline.Tick(dag, now)
	if ev.FreshnessBand != domain.FreshnessGreen || ev.Overall != domain.OverallTrusted {
		t.Fatalf("%+v", ev)
	}
}

func TestRunningPastTwoXFailed(t *testing.T) {
	now := time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)
	start := now.Add(-5 * time.Hour)
	dag := domain.DAG{
		Status: "RUNNING", StartedAt: ts(start),
		SLAMinutes: f64(30), SilentMonitored: true, IntervalMins: f64(60),
	}
	ev := pipeline.Tick(dag, now)
	if ev.Overall != domain.OverallFailed {
		t.Fatalf("got %s %s", ev.Overall, ev.Description)
	}
}
