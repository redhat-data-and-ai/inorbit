package pipeline

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/inorbit/inorbit/internal/domain"
)

const (
	reliabilityHealthy = 95.0
	reliabilityCaution = 80.0
)

// RunFact is one Airflow dag run used for 7/30/90d success rate.
type RunFact struct {
	Started time.Time
	Success bool
}

// FrequencyDisplay matches marts.pipeline_status.dag_frequency_display.
func FrequencyDisplay(intervalMins *float64) string {
	if intervalMins == nil || *intervalMins <= 0 {
		return ""
	}
	m := *intervalMins
	switch {
	case m < 60:
		return fmt.Sprintf("Every %.0fm", math.Round(m))
	case m < 1440:
		return fmt.Sprintf("Every %.0fhr", math.Floor(m/60))
	case m == 1440:
		return "Every 24hr"
	default:
		return fmt.Sprintf("Every %.0fd", math.Floor(m/1440))
	}
}

// TriggerFromRunID maps Airflow dag_run_id / run_type to SCHEDULED, MANUAL, etc.
func TriggerFromRunID(runID, runType string) string {
	if t := strings.ToUpper(strings.TrimSpace(runType)); t != "" {
		t = strings.ReplaceAll(t, "-", "_")
		switch t {
		case "DATASET_TRIGGERED", "ASSET_TRIGGERED":
			return "DATASET"
		default:
			return t
		}
	}
	id := strings.ToLower(runID)
	switch {
	case strings.HasPrefix(id, "scheduled__"):
		return "SCHEDULED"
	case strings.HasPrefix(id, "manual__"):
		return "MANUAL"
	case strings.Contains(id, "dataset"):
		return "DATASET"
	default:
		return ""
	}
}

// ReliabilityBand maps a 0–100 success rate to TRUSTED / CAUTION / STALE / UNKNOWN.
func ReliabilityBand(pct *float64) string {
	if pct == nil {
		return "UNKNOWN"
	}
	switch {
	case *pct >= reliabilityHealthy:
		return "TRUSTED"
	case *pct >= reliabilityCaution:
		return "CAUTION"
	default:
		return "STALE"
	}
}

// ApplyReliability fills 7/30/90d success rates from recent Airflow runs.
func ApplyReliability(d *domain.DAG, runs []RunFact, now time.Time) {
	now = now.UTC()
	n7, ok7 := window(runs, now, 7)
	n30, ok30 := window(runs, now, 30)
	n90, ok90 := window(runs, now, 90)
	d.Runs7d, d.Reliability7d, d.ReliabilityStatus7d = packRel(n7, ok7)
	d.Runs30d, d.Reliability30d, d.ReliabilityStatus30d = packRel(n30, ok30)
	d.Runs90d, d.Reliability90d, d.ReliabilityStatus90d = packRel(n90, ok90)
}

func window(runs []RunFact, now time.Time, days int) (n, ok int) {
	start := now.AddDate(0, 0, -days)
	for _, r := range runs {
		if r.Started.IsZero() || r.Started.Before(start) {
			continue
		}
		n++
		if r.Success {
			ok++
		}
	}
	return n, ok
}

func packRel(n, ok int) (int, *float64, string) {
	if n == 0 {
		return 0, nil, "UNKNOWN"
	}
	pct := math.Round(10000*float64(ok)/float64(n)) / 100
	return n, &pct, ReliabilityBand(&pct)
}

// EnrichDAG fills display fields the UI expects (frequency, type, trigger, reliability bands).
func EnrichDAG(d *domain.DAG) {
	if d.PipelineType == "" {
		d.PipelineType = "DAG"
	}
	if d.FrequencyDisplay == "" {
		d.FrequencyDisplay = FrequencyDisplay(d.IntervalMins)
	}
	if strings.TrimSpace(d.TriggerType) == "" {
		d.TriggerType = TriggerFromRunID(d.RunID, "")
	} else {
		d.TriggerType = TriggerFromRunID(d.RunID, d.TriggerType)
	}
	if d.ReliabilityStatus7d == "" {
		d.ReliabilityStatus7d = ReliabilityBand(d.Reliability7d)
	}
	if d.ReliabilityStatus30d == "" {
		d.ReliabilityStatus30d = ReliabilityBand(d.Reliability30d)
	}
	if d.ReliabilityStatus90d == "" {
		d.ReliabilityStatus90d = ReliabilityBand(d.Reliability90d)
	}
}
