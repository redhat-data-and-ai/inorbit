package engine

import (
	"fmt"
	"time"

	"github.com/inorbit/inorbit/internal/domain"
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
		dags := e.Store.DAGs(dp.ID)
		rawChecks := e.Store.Checks(dp.ID)
		checks := make([]domain.Check, 0, len(rawChecks)+len(dags))
		for _, c := range rawChecks {
			c.DataProductID = dp.ID
			c.DataProductName = dp.Name
			checks = append(checks, score.Apply(c, now, e.Score))
		}

		pipe := make([]domain.PipelineStatus, 0, len(dags))
		for _, dag := range dags {
			pipeline.EnrichDAG(&dag)
			ev := pipeline.Tick(dag, now)
			ps := domain.PipelineStatus{
				DAG:                         dag,
				DAGFreshnessStatus:          ev.FreshnessLabel,
				DAGPipelineSLAStatus:        ev.PipelineSLA,
				DAGOverallStatus:            ev.Overall,
				DAGOverallStatusDescription: ev.Description,
				DAGDataAgeMins:              ev.DataAgeMins,
				ComputedAt:                  now,
			}
			pipe = append(pipe, ps)
			checks = append(checks, virtualChecks(dp, dag, ev, now)...)
		}

		for i := range checks {
			checks[i] = score.Apply(checks[i], now, e.Score)
		}

		health := score.Evaluate([]domain.DataProduct{dp}, checks, e.Score)[0]
		health.EvaluatedAt = now
		fresh := pipeline.RollupFreshness(dp, dags, now)

		e.Store.PutSnapshot(domain.Snapshot{
			DataProduct: dp,
			Health:      health,
			Freshness:   fresh,
			Pipeline:    pipe,
			Quality:     checks,
			UpdatedAt:   now,
		})
	}
	e.Store.TouchClock(now)
}

func virtualChecks(dp domain.DataProduct, dag domain.DAG, ev pipeline.DAGEval, now time.Time) []domain.Check {
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
			ID:              fmt.Sprintf("%s:%s:freshness", dp.ID, dag.DAGID),
			DataProductID:   dp.ID,
			DataProductName: dp.Name,
			Name:            "Astro freshness " + dag.DAGID,
			Dimension:       domain.DimFreshness,
			Severity:        sev,
			Status:          st,
			SourceType:      domain.SrcAstroFreshness,
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
			ID:              fmt.Sprintf("%s:%s:pipeline", dp.ID, dag.DAGID),
			DataProductID:   dp.ID,
			DataProductName: dp.Name,
			Name:            "Astro pipeline " + dag.DAGID,
			Dimension:       domain.DimFreshness,
			Severity:        domain.SevCritical,
			Status:          domain.CheckFailed,
			SourceType:      domain.SrcAstroPipeline,
			SourceTable:     dag.DAGID,
			ExecutedAt:      now,
			FirstFailedAt:   &failAt,
		})
	}
	return out
}
