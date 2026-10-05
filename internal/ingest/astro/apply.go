package astro

import (
	"strings"
	"time"

	"github.com/inorbit/inorbit/internal/domain"
	"github.com/inorbit/inorbit/internal/store"
)

// Apply writes ingested DAGs into the snapshot store.
// Products with no matching DAGs keep an empty pipeline list.
func Apply(st *store.Memory, dags []domain.DAG) {
	byDP := map[string][]domain.DAG{}
	for _, d := range dags {
		byDP[d.DataProductID] = append(byDP[d.DataProductID], d)
	}
	for _, p := range st.Products() {
		st.SetDAGs(p.ID, dedupeLatest(byDP[p.ID]))
	}
}

// MergeLiveAndMart prefers live Airflow rows, then adds warehouse-mart
// pipelines the live poll did not return.
func MergeLiveAndMart(live, mart []domain.DAG) []domain.DAG {
	seen := map[string]struct{}{}
	k := func(d domain.DAG) string {
		product := strings.ToLower(strings.TrimSpace(d.DataProductID))
		return product + "\x00" + pipelineIdentity(d)
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

// dedupeLatest keeps one row per pipeline identity. Airflow still collapses
// the same dag_id across deployments to the newest run. Fivetran keeps one
// row per destination + schema so prod and stage connectors both stay.
func dedupeLatest(dags []domain.DAG) []domain.DAG {
	best := map[string]domain.DAG{}
	order := make([]string, 0)
	for _, d := range dags {
		key := pipelineIdentity(d)
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

func pipelineIdentity(d domain.DAG) string {
	kind := strings.ToUpper(strings.TrimSpace(d.PipelineType))
	if kind == "" {
		kind = domain.PipelineTypeDAG
	}
	id := strings.ToLower(strings.TrimSpace(d.DAGID))
	if kind == domain.PipelineTypeFivetran {
		return kind + "\x00" + strings.ToLower(strings.TrimSpace(d.DeploymentName)) + "\x00" + id
	}
	return kind + "\x00" + id
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
