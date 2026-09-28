package lineage

import (
	"time"

	"github.com/inorbit/inorbit/internal/domain"
	"github.com/inorbit/inorbit/internal/engine"
	"github.com/inorbit/inorbit/internal/store"
)

// Apply writes warehouse lineage into the snapshot store and recomputes.
func Apply(st *store.Memory, eng *engine.Engine, rows []domain.Lineage, now time.Time) {
	by := map[string]domain.Lineage{}
	for _, lin := range rows {
		by[lin.DataProductID] = lin
	}
	for _, p := range st.Products() {
		st.SetLineage(p.ID, by[p.ID])
	}
	eng.Recompute(now)
}
