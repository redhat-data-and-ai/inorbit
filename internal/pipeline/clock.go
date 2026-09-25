package pipeline

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/inorbit/inorbit/internal/domain"
)

const runningLagBufferMins = 15

type DAGEval struct {
	FreshnessBand  domain.FreshnessBand
	PipelineSLA    domain.PipelineSLA
	Overall        domain.OverallStatus
	Description    string
	DataAgeMins    *float64
	FreshnessLabel domain.HealthLabel
}

// Tick is the SLA clock: wall clock vs last run + DAG tags. No I/O.
func Tick(dag domain.DAG, now time.Time) DAGEval {
	now = now.UTC()
	age := dataAgeMins(dag, now)
	fresh := freshnessBand(dag, now)
	sla := pipelineSLA(dag, now)
	overall, desc := overallStatus(dag, now, fresh, sla, age)
	return DAGEval{
		FreshnessBand:  fresh,
		PipelineSLA:    sla,
		Overall:        overall,
		Description:    desc,
		DataAgeMins:    age,
		FreshnessLabel: bandToHealth(fresh),
	}
}

func dataAgeMins(dag domain.DAG, now time.Time) *float64 {
	ref := dag.CompletedAt
	if ref == nil {
		ref = dag.StartedAt
	}
	if ref == nil {
		return nil
	}
	v := now.Sub(ref.UTC()).Minutes()
	if v < 0 {
		v = 0
	}
	return &v
}

func slaMins(dag domain.DAG) float64 {
	if dag.SLAMinutes != nil && *dag.SLAMinutes > 0 {
		return *dag.SLAMinutes
	}
	return 120
}

func freshnessBand(dag domain.DAG, now time.Time) domain.FreshnessBand {
	if dag.IntervalMins == nil && dag.SLAMinutes == nil && dag.NextExpectedAt == nil {
		return domain.FreshnessGreen
	}
	if strings.EqualFold(dag.Status, "FAILED") {
		return domain.FreshnessRed
	}
	if dag.NextExpectedAt == nil {
		return domain.FreshnessGreen
	}
	deadline := dag.NextExpectedAt.UTC().Add(time.Duration(slaMins(dag)) * time.Minute)
	if now.After(deadline) {
		return domain.FreshnessRed
	}
	if now.After(dag.NextExpectedAt.UTC()) {
		return domain.FreshnessYellow
	}
	return domain.FreshnessGreen
}

func pipelineSLA(dag domain.DAG, now time.Time) domain.PipelineSLA {
	if dag.SLAMinutes == nil {
		return domain.SLAOK
	}
	sla := *dag.SLAMinutes
	st := strings.ToUpper(dag.Status)
	if st == "RUNNING" && dag.StartedAt != nil {
		runMins := now.Sub(dag.StartedAt.UTC()).Minutes()
		if runMins > sla+runningLagBufferMins {
			return domain.SLABreach
		}
		if runMins > sla {
			return domain.SLAAtRisk
		}
		return domain.SLAOK
	}
	durMins := dag.DurationSeconds / 60
	if durMins > sla {
		return domain.SLABreach
	}
	if durMins > sla*0.8 {
		return domain.SLAAtRisk
	}
	return domain.SLAOK
}

func overallStatus(dag domain.DAG, now time.Time, fresh domain.FreshnessBand, sla domain.PipelineSLA, age *float64) (domain.OverallStatus, string) {
	st := strings.ToUpper(dag.Status)
	slaM := slaMins(dag)
	ageStr := "unknown"
	if age != nil {
		ageStr = FormatMins(*age)
	}

	if st == "FAILED" {
		msg := "DAG failed"
		if dag.ErrorMessage != "" {
			msg += " — " + dag.ErrorMessage
		}
		return domain.OverallFailed, msg
	}
	if dag.IsPaused && dag.IsCustom {
		return domain.OverallPaused, "Custom DAG is paused"
	}
	if dag.IsPaused && dag.IntervalMins == nil {
		return domain.OverallPaused, "DAG is paused (manual-only, no defined schedule)"
	}
	if dag.IsPaused && dag.NextExpectedAt != nil {
		overdue := now.Sub(dag.NextExpectedAt.UTC()).Minutes()
		if overdue > slaM*2 {
			return domain.OverallFailed, fmt.Sprintf("DAG is paused and critically overdue — data is %s old (2x SLA exceeded)", ageStr)
		}
		if fresh == domain.FreshnessRed {
			return domain.OverallDelayed, fmt.Sprintf("DAG is paused and missed expected scheduled run — data is %s old", ageStr)
		}
	}
	if st == "RUNNING" && dag.SilentMonitored && dag.StartedAt != nil {
		runMins := now.Sub(dag.StartedAt.UTC()).Minutes()
		if runMins > slaM*2 {
			return domain.OverallFailed, fmt.Sprintf("DAG is still running and critically overdue — running for %.0fm (2x SLA exceeded)", math.Round(runMins))
		}
		if sla == domain.SLABreach {
			return domain.OverallDelayed, fmt.Sprintf("DAG is still running but has breached SLA — running for %.0fm", math.Round(runMins))
		}
	}
	if st == "RUNNING" {
		return domain.OverallRunning, ""
	}
	if fresh == domain.FreshnessRed {
		return domain.OverallAtRisk, fmt.Sprintf("Data SLA breached — data is %s old", ageStr)
	}
	if sla == domain.SLABreach {
		return domain.OverallAtRisk, fmt.Sprintf("Pipeline SLA breach — duration %.0fm exceeds threshold", math.Round(dag.DurationSeconds/60))
	}
	if sla == domain.SLAAtRisk {
		return domain.OverallCaution, "Pipeline duration approaching SLA threshold"
	}
	if fresh == domain.FreshnessYellow {
		return domain.OverallCaution, "Data freshness approaching SLA threshold"
	}
	return domain.OverallTrusted, "Healthy"
}

func bandToHealth(b domain.FreshnessBand) domain.HealthLabel {
	switch b {
	case domain.FreshnessRed:
		return domain.HealthAtRisk
	case domain.FreshnessYellow:
		return domain.HealthCaution
	default:
		return domain.HealthTrusted
	}
}

func RollupFreshness(dp domain.DataProduct, dags []domain.DAG, now time.Time) domain.FreshnessSLA {
	out := domain.FreshnessSLA{
		DataProductID:     dp.ID,
		DataProductName:   dp.Name,
		FreshnessStatus:   domain.FreshnessGreen,
		PipelineSLABreach: domain.SLAOK,
		CheckedAt:         now.UTC(),
	}
	if len(dags) == 0 {
		out.StatusReason = "No DAGs monitored"
		return out
	}
	var worstBand domain.FreshnessBand = domain.FreshnessGreen
	var worstSLA domain.PipelineSLA = domain.SLAOK
	var delay *float64
	for _, d := range dags {
		if d.IsCustom {
			continue
		}
		ev := Tick(d, now)
		if rankBand(ev.FreshnessBand) > rankBand(worstBand) {
			worstBand = ev.FreshnessBand
			out.StatusReason = ev.Description
			delay = ev.DataAgeMins
		}
		if rankSLA(ev.PipelineSLA) > rankSLA(worstSLA) {
			worstSLA = ev.PipelineSLA
		}
		if d.IsPrimary {
			out.ExpectedIntervalMins = d.IntervalMins
			out.SLAMinutes = d.SLAMinutes
			out.LastSuccessfulAt = d.LastSuccessAt
			if d.LastSuccessAt == nil {
				out.LastSuccessfulAt = d.CompletedAt
			}
		}
	}
	out.FreshnessStatus = worstBand
	out.PipelineSLABreach = worstSLA
	out.CurrentDelayMins = delay
	if out.StatusReason == "" {
		switch worstBand {
		case domain.FreshnessRed:
			out.StatusReason = "Freshness SLA breached"
		case domain.FreshnessYellow:
			out.StatusReason = "Approaching next expected run + SLA"
		default:
			out.StatusReason = "On schedule"
		}
	}
	return out
}

func rankBand(b domain.FreshnessBand) int {
	switch b {
	case domain.FreshnessRed:
		return 2
	case domain.FreshnessYellow:
		return 1
	default:
		return 0
	}
}

func rankSLA(s domain.PipelineSLA) int {
	switch s {
	case domain.SLABreach:
		return 2
	case domain.SLAAtRisk:
		return 1
	default:
		return 0
	}
}
