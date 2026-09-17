package astro

import (
	"strings"
	"time"

	"github.com/inorbit/inorbit/internal/domain"
	"github.com/inorbit/inorbit/internal/engine"
	"github.com/inorbit/inorbit/internal/store"
)

// Apply writes ingested DAGs into the snapshot store and recomputes.
// Products with no matching DAGs keep an empty pipeline list.
func Apply(st *store.Memory, eng *engine.Engine, dags []domain.DAG, now time.Time) {
	byDP := map[string][]domain.DAG{}
	for _, d := range dags {
		byDP[d.DataProductID] = append(byDP[d.DataProductID], d)
	}
	for _, p := range st.Products() {
		st.SetDAGs(p.ID, dedupeLatest(byDP[p.ID]))
	}
	eng.Recompute(now)
}

// dedupeLatest keeps one row per dag_id (Ludo stg_astro_pipeline): the
// deployment whose latest run is newest. Empty runs lose to a run.
func dedupeLatest(dags []domain.DAG) []domain.DAG {
	best := map[string]domain.DAG{}
	order := make([]string, 0)
	for _, d := range dags {
		key := strings.ToLower(strings.TrimSpace(d.DAGID))
		prev, ok := best[key]
		if !ok {
			order = append(order, key)
			best[key] = d
			continue
		}
		if dagNewer(d, prev) {
			best[key] = d
		}
	}
	out := make([]domain.DAG, 0, len(order))
	for _, k := range order {
		out = append(out, best[k])
	}
	return out
}

func dagNewer(a, b domain.DAG) bool {
	return dagTime(a).After(dagTime(b))
}

func dagTime(d domain.DAG) time.Time {
	if d.CompletedAt != nil {
		return *d.CompletedAt
	}
	if d.StartedAt != nil {
		return *d.StartedAt
	}
	return time.Time{}
}
