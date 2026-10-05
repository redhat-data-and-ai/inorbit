package engine

import (
	"fmt"
	"strings"
	"time"

	"github.com/inorbit/inorbit/internal/domain"
	"github.com/inorbit/inorbit/internal/ingest/fivetran"
	"github.com/inorbit/inorbit/internal/pipeline"
	"github.com/inorbit/inorbit/internal/score"
	"github.com/inorbit/inorbit/internal/store"
)

type Engine struct {
	Store *store.Memory
	Score score.Config
}

func New(st *store.Memory) *Engine {
	return &Engine{Store: st, Score: score.DefaultConfig()}
}

// Recompute runs the SLA clock + health score over the in-memory ingest state.
func (e *Engine) Recompute(now time.Time) {
	now = now.UTC()
	products := e.Store.Products()
	for _, dp := range products {
		stored := e.Store.DAGs(dp.ID)
		conns := e.Store.Connectors(dp.ID)
		dags := make([]domain.DAG, 0, len(stored))
		scoreDAGs := make([]domain.DAG, 0, len(stored)+len(conns))
		for _, d := range stored {
			if strings.EqualFold(d.PipelineType, domain.PipelineTypeFivetran) {
				scoreDAGs = append(scoreDAGs, d)
				continue
			}
			dags = append(dags, d)
			scoreDAGs = append(scoreDAGs, d)
		}
		scoreDAGs = append(scoreDAGs, fivetran.AsDAGs(conns)...)
		rawChecks := e.Store.Checks(dp.ID)
		checks := make([]domain.Check, 0, len(rawChecks)+len(scoreDAGs))
		for _, c := range rawChecks {
			c.DataProductID = dp.ID
			c.DataProductName = dp.Name
			checks = append(checks, score.Apply(c, now, e.Score))
		}

		pipe := make([]domain.PipelineStatus, 0, len(dags))
		hasProd := pipeline.HasProduction(scoreDAGs)
		for _, dag := range scoreDAGs {
			pipeline.EnrichDAG(&dag)
			ev := pipeline.Tick(dag, now)
			if !strings.EqualFold(dag.PipelineType, domain.PipelineTypeFivetran) {
				pipe = append(pipe, domain.PipelineStatus{
					DAG:                         dag,
					DAGFreshnessStatus:          ev.FreshnessLabel,
					DAGPipelineSLAStatus:        ev.PipelineSLA,
					DAGOverallStatus:            ev.Overall,
					DAGOverallStatusDescription: ev.Description,
					DAGDataAgeMins:              ev.DataAgeMins,
					ComputedAt:                  now,
				})
			}
			if pipeline.ScoreDAG(dag, hasProd) {
				checks = append(checks, virtualChecks(dp, dag, ev, now)...)
			}
		}

		for i := range checks {
			checks[i] = score.Apply(checks[i], now, e.Score)
		}

		health := score.Evaluate([]domain.DataProduct{dp}, checks, e.Score)[0]
		if health.TotalChecks == 0 {
			if pt, ok := e.Store.LatestMartHealth(dp.ID); ok {
				applyMartHealth(&health, pt)
			}
		}
		health.EvaluatedAt = now
		fresh := pipeline.RollupFreshness(dp, scoreDAGs, now)
		lin := e.Store.Lineage(dp.ID)
		lin.DataProductID = dp.ID
		lin.DataProductName = dp.Name
		if lin.UpstreamSources == nil {
			lin.UpstreamSources = []domain.LineageNode{}
		}
		if lin.DownstreamConsumers == nil {
			lin.DownstreamConsumers = []domain.LineageNode{}
		}
		if fivetran.SourceAligned(dp.Type) {
			lin.UpstreamSources = fivetran.EnrichUpstream(lin.UpstreamSources, conns)
			lin.UpstreamCount = len(lin.UpstreamSources)
		}

		e.Store.PutSnapshot(domain.Snapshot{
			DataProduct:    dp,
			Health:         health,
			Freshness:      fresh,
			Pipeline:       pipe,
			Connectors:     conns,
			Quality:        checks,
			QualitySources: e.Store.QualitySources(dp.ID),
			Lineage:        lin,
			UpdatedAt:      now,
		})
	}
	e.Store.TouchClock(now)
}

func virtualChecks(dp domain.DataProduct, dag domain.DAG, ev pipeline.DAGEval, now time.Time) []domain.Check {
	srcFresh, srcPipe := domain.SrcAstroFreshness, domain.SrcAstroPipeline
	nameFresh, namePipe := "Astro freshness ", "Astro pipeline "
	if strings.EqualFold(dag.PipelineType, domain.PipelineTypeFivetran) {
		srcFresh, srcPipe = domain.SrcFivetranFreshness, domain.SrcFivetranPipeline
		nameFresh, namePipe = "Fivetran freshness ", "Fivetran sync "
	}
	var out []domain.Check
	if ev.FreshnessBand == domain.FreshnessRed || ev.FreshnessBand == domain.FreshnessYellow {
		st := domain.CheckFailed
		sev := domain.SevHigh
		if ev.FreshnessBand == domain.FreshnessYellow {
			st = domain.CheckWarning
			sev = domain.SevMedium
		}
		failAt := now
		out = append(out, domain.Check{
			ID:              fmt.Sprintf("%s:%s:%s:freshness", dp.ID, dag.DeploymentName, dag.DAGID),
			DataProductID:   dp.ID,
			DataProductName: dp.Name,
			Name:            nameFresh + dag.DAGID,
			Dimension:       domain.DimFreshness,
			Severity:        sev,
			Status:          st,
			SourceType:      srcFresh,
			SourceTable:     dag.DAGID,
			ExecutedAt:      now,
			FirstFailedAt:   &failAt,
		})
	}
	if ev.Overall == domain.OverallFailed && dag.Status == "FAILED" {
		failAt := now
		if dag.CompletedAt != nil {
			failAt = *dag.CompletedAt
		}
		out = append(out, domain.Check{
			ID:              fmt.Sprintf("%s:%s:%s:pipeline", dp.ID, dag.DeploymentName, dag.DAGID),
			DataProductID:   dp.ID,
			DataProductName: dp.Name,
			Name:            namePipe + dag.DAGID,
			Dimension:       domain.DimFreshness,
			Severity:        domain.SevCritical,
			Status:          domain.CheckFailed,
			SourceType:      srcPipe,
			SourceTable:     dag.DAGID,
			ExecutedAt:      now,
			FirstFailedAt:   &failAt,
		})
	}
	return out
}

func applyMartHealth(h *domain.HealthStatus, pt domain.HealthPoint) {
	h.HealthScore = pt.Score
	h.Status = pt.Status
	h.TotalChecks = pt.TotalChecks
	h.FailedChecks = pt.FailedChecks
	h.FreshnessScore = pt.FreshnessScore
	h.AccuracyScore = pt.AccuracyScore
	h.ConsistencyScore = pt.ConsistencyScore
	h.CompletenessScore = pt.CompletenessScore
	h.ValidityScore = pt.ValidityScore
	h.UniquenessScore = pt.UniquenessScore
	h.MeasuredDimensionCount = pt.MeasuredDimensionCount
	if pt.MeasuredDimensionCount > 0 {
		h.CoveragePct = float64(pt.MeasuredDimensionCount) / 6.0 * 100
	}
	switch {
	case pt.TotalChecks > 0 && pt.FailedChecks == 0:
		h.StatusMessage = "All checks passing"
	case pt.FailedChecks > 0:
		h.StatusMessage = fmt.Sprintf("%d failed check(s)", pt.FailedChecks)
	default:
		h.StatusMessage = "Warehouse health snapshot"
	}
}
