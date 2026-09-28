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

// MergeLiveAndMart prefers live Airflow rows, then adds warehouse-mart DAGs
// for products (or dag_ids) the listed deployments did not return.
func MergeLiveAndMart(live, mart []domain.DAG) []domain.DAG {
	type key struct{ product, dag string }
	seen := map[key]struct{}{}
	k := func(d domain.DAG) key {
		return key{
			product: strings.ToLower(strings.TrimSpace(d.DataProductID)),
			dag:     strings.ToLower(strings.TrimSpace(d.DAGID)),
		}
	}
	out := make([]domain.DAG, 0, len(live)+len(mart))
	for _, d := range live {
		if strings.TrimSpace(d.DAGID) == "" {
			continue
		}
		seen[k(d)] = struct{}{}
		out = append(out, d)
	}
	for _, d := range mart {
		if strings.TrimSpace(d.DAGID) == "" {
			continue
		}
		id := k(d)
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, d)
	}
	return out
}

// dedupeLatest keeps one row per dag_id: the deployment whose latest run
// is newest. Empty runs lose to a run.
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
