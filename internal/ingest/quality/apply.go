package quality

import (
	"time"

	"github.com/inorbit/inorbit/internal/domain"
	"github.com/inorbit/inorbit/internal/engine"
	"github.com/inorbit/inorbit/internal/store"
)

// Apply writes ingested quality checks for the products in this batch and recomputes health.
func Apply(st *store.Memory, eng *engine.Engine, checks []domain.Check, productIDs []string, now time.Time) {
	byDP := map[string][]domain.Check{}
	for _, c := range checks {
		byDP[c.DataProductID] = append(byDP[c.DataProductID], c)
	}
	ids := productIDs
	if len(ids) == 0 {
		for id := range byDP {
			ids = append(ids, id)
		}
	}
	for _, id := range ids {
		st.SetChecks(id, byDP[id])
	}
	eng.Recompute(now)
}
