package pipeline_test

import (
	"testing"
	"time"

	"github.com/inorbit/inorbit/internal/domain"
	"github.com/inorbit/inorbit/internal/pipeline"
)

func TestFrequencyDisplay(t *testing.T) {
	cases := []struct {
		mins float64
		want string
	}{
		{15, "Every 15m"},
		{60, "Every 1hr"},
		{240, "Every 4hr"},
		{1440, "Every 24hr"},
		{2880, "Every 2d"},
	}
	for _, tc := range cases {
		m := tc.mins
		if got := pipeline.FrequencyDisplay(&m); got != tc.want {
			t.Fatalf("mins=%v got %q want %q", tc.mins, got, tc.want)
		}
	}
	if pipeline.FrequencyDisplay(nil) != "" {
		t.Fatal("nil")
	}
}

func TestTriggerFromRunID(t *testing.T) {
	if got := pipeline.TriggerFromRunID("scheduled__2026-09-17T04:10:00+00:00", ""); got != "SCHEDULED" {
		t.Fatalf("got %q", got)
	}
	if got := pipeline.TriggerFromRunID("manual__x", "dataset_triggered"); got != "DATASET" {
		t.Fatalf("got %q", got)
	}
	if got := pipeline.TriggerFromRunID("manual__x", ""); got != "MANUAL" {
		t.Fatalf("got %q", got)
	}
}

func TestApplyReliabilityWindows(t *testing.T) {
	now := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	runs := []pipeline.RunFact{
		{Started: now.Add(-24 * time.Hour), Success: true},
		{Started: now.Add(-48 * time.Hour), Success: true},
		{Started: now.Add(-10 * 24 * time.Hour), Success: false},
		{Started: now.Add(-40 * 24 * time.Hour), Success: true},
	}
	var d domain.DAG
	pipeline.ApplyReliability(&d, runs, now)
	if d.Runs7d != 2 || d.Reliability7d == nil || *d.Reliability7d != 100 {
		t.Fatalf("7d %+v %v", d.Runs7d, d.Reliability7d)
	}
	if d.Runs30d != 3 || d.Reliability30d == nil || *d.Reliability30d != 66.67 {
		t.Fatalf("30d %+v %v", d.Runs30d, d.Reliability30d)
	}
	if d.Runs90d != 4 || d.Reliability90d == nil || *d.Reliability90d != 75 {
		t.Fatalf("90d %+v %v", d.Runs90d, d.Reliability90d)
	}
	if d.ReliabilityStatus7d != "TRUSTED" || d.ReliabilityStatus30d != "STALE" {
		t.Fatalf("bands %s %s", d.ReliabilityStatus7d, d.ReliabilityStatus30d)
	}
}

func TestEnrichDAGFillsDisplay(t *testing.T) {
	m := 1440.0
	pct := 81.08
	d := domain.DAG{IntervalMins: &m, RunID: "scheduled__orders", Reliability30d: &pct}
	pipeline.EnrichDAG(&d)
	if d.PipelineType != "DAG" || d.FrequencyDisplay != "Every 24hr" || d.TriggerType != "SCHEDULED" {
		t.Fatalf("%+v", d)
	}
	if d.ReliabilityStatus30d != "CAUTION" {
		t.Fatalf("band %s", d.ReliabilityStatus30d)
	}
}
