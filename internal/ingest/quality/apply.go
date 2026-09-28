package quality

import (
	"strings"
	"time"

	"github.com/inorbit/inorbit/internal/domain"
	"github.com/inorbit/inorbit/internal/engine"
	"github.com/inorbit/inorbit/internal/store"
)

// ConfigSources marks catalog quality tables as present (demo) or disabled.
func ConfigSources(p domain.DataProduct) domain.QualitySources {
	return domain.QualitySources{
		Validation: configTable(p.Validation),
		DBT:        configTable(p.DBTLogs),
	}
}

func configTable(t domain.QualityTable) domain.QualitySource {
	if !t.Enabled {
		return domain.QualitySource{Status: domain.QualityDisabled}
	}
	parts := []string{t.Database, t.Schema, t.Table}
	for i, p := range parts {
		parts[i] = strings.ToUpper(strings.TrimSpace(p))
	}
	return domain.QualitySource{Status: domain.QualityOK, Relation: strings.Join(parts, ".")}
}

// Apply writes ingested quality checks and warehouse-table availability, then recomputes health.
func Apply(st *store.Memory, eng *engine.Engine, checks []domain.Check, sources map[string]domain.QualitySources, now time.Time) {
	byDP := map[string][]domain.Check{}
	for _, c := range checks {
		byDP[c.DataProductID] = append(byDP[c.DataProductID], c)
	}
	ids := make([]string, 0, len(sources))
	for id := range sources {
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		for id := range byDP {
			ids = append(ids, id)
		}
	}
	for _, id := range ids {
		st.SetChecks(id, byDP[id])
		if src, ok := sources[id]; ok {
			st.SetQualitySources(id, src)
		}
	}
	eng.Recompute(now)
}
