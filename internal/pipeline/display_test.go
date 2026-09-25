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
		{15, "Every 15 min"},
		{60, "Every 1 hour"},
		{90, "Every 1 hour 30 min"},
		{240, "Every 4 hours"},
		{1440, "Every 1 day"},
		{2880, "Every 2 days"},
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

func TestFormatMins(t *testing.T) {
	if got := pipeline.FormatMins(45); got != "45 min" {
		t.Fatalf("45: %q", got)
	}
	if got := pipeline.FormatMins(120); got != "2 hours" {
		t.Fatalf("120: %q", got)
	}
	if got := pipeline.FormatMins(1500); got != "1 day 1 hour" {
		t.Fatalf("1500: %q", got)
	}
}

func TestApplySLADefaultsQuarterIntervalMin30(t *testing.T) {
	m := 1440.0
	d := domain.DAG{IntervalMins: &m}
	pipeline.ApplySLADefaults(&d)
	if d.SLAMinutes == nil || *d.SLAMinutes != 360 {
		t.Fatalf("daily sla %+v", d.SLAMinutes)
	}
	hourly := 60.0
	h := domain.DAG{IntervalMins: &hourly}
	pipeline.ApplySLADefaults(&h)
	if h.SLAMinutes == nil || *h.SLAMinutes != 30 {
		t.Fatalf("hourly floor 30, got %+v", h.SLAMinutes)
	}
}

func TestEnrichDAGFillsDisplay(t *testing.T) {
	m := 1440.0
	pct := 81.08
	d := domain.DAG{IntervalMins: &m, RunID: "scheduled__orders", Reliability30d: &pct}
	pipeline.EnrichDAG(&d)
	if d.PipelineType != "DAG" || d.FrequencyDisplay != "Every 1 day" || d.TriggerType != "SCHEDULED" {
		t.Fatalf("%+v", d)
	}
	if d.ReliabilityStatus30d != "CAUTION" {
		t.Fatalf("band %s", d.ReliabilityStatus30d)
	}
}
