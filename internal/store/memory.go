package store

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/inorbit/inorbit/internal/domain"
)

// Memory holds the live snapshot for every data product. Not history.
type Memory struct {
	mu        sync.RWMutex
	products  map[string]domain.DataProduct
	dags      map[string][]domain.DAG
	livePipe  []domain.DAG
	martPipe  []domain.DAG
	conns     map[string][]domain.Connector
	checks    map[string][]domain.Check
	qsrc      map[string]domain.QualitySources
	lineage   map[string]domain.Lineage
	snaps     map[string]domain.Snapshot
	hist      map[string][]domain.HealthPoint
	subs      map[string]domain.Subscription
	snapBytes map[string]int
	meta      domain.PollMeta
}

func New() *Memory {
	return &Memory{
		products:  map[string]domain.DataProduct{},
		dags:      map[string][]domain.DAG{},
		livePipe:  []domain.DAG{},
		martPipe:  []domain.DAG{},
		conns:     map[string][]domain.Connector{},
		checks:    map[string][]domain.Check{},
		qsrc:      map[string]domain.QualitySources{},
		lineage:   map[string]domain.Lineage{},
		snaps:     map[string]domain.Snapshot{},
		hist:      map[string][]domain.HealthPoint{},
		subs:      map[string]domain.Subscription{},
		snapBytes: map[string]int{},
	}
}

func (m *Memory) UpsertProduct(p domain.DataProduct) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.products[p.ID] = p
}

func (m *Memory) SetDAGs(dataProductID string, dags []domain.DAG) {
	m.mu.Lock()
	defer m.mu.Unlock()
	copied := append([]domain.DAG(nil), dags...)
	m.dags[dataProductID] = copied
}

func (m *Memory) SetLivePipeline(dags []domain.DAG) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.livePipe = append([]domain.DAG(nil), dags...)
}

func (m *Memory) LivePipeline() []domain.DAG {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]domain.DAG(nil), m.livePipe...)
}

func (m *Memory) SetConnectors(dataProductID string, conns []domain.Connector) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.conns[dataProductID] = append([]domain.Connector(nil), conns...)
}

func (m *Memory) Connectors(id string) []domain.Connector {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]domain.Connector(nil), m.conns[id]...)
}

func (m *Memory) SetMartPipeline(dags []domain.DAG) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.martPipe = append([]domain.DAG(nil), dags...)
}

func (m *Memory) MartPipeline() []domain.DAG {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]domain.DAG(nil), m.martPipe...)
}

func (m *Memory) SetChecks(dataProductID string, checks []domain.Check) {
	m.mu.Lock()
	defer m.mu.Unlock()
	copied := append([]domain.Check(nil), checks...)
	m.checks[dataProductID] = copied
}

func (m *Memory) SetQualitySources(dataProductID string, src domain.QualitySources) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.qsrc[dataProductID] = src
}

func (m *Memory) QualitySources(id string) domain.QualitySources {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.qsrc[id]
}

func (m *Memory) SetLineage(dataProductID string, lin domain.Lineage) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if lin.UpstreamSources == nil {
		lin.UpstreamSources = []domain.LineageNode{}
	}
	if lin.DownstreamConsumers == nil {
		lin.DownstreamConsumers = []domain.LineageNode{}
	}
	m.lineage[dataProductID] = lin
}

func (m *Memory) Lineage(id string) domain.Lineage {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lineage[id]
}

func (m *Memory) PutSnapshot(s domain.Snapshot) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snaps[s.DataProduct.ID] = s
	if b, err := json.Marshal(s); err == nil {
		m.snapBytes[s.DataProduct.ID] = len(b)
	}
	pt := domain.HealthPoint{At: s.UpdatedAt, Score: s.Health.HealthScore, Status: s.Health.Status}
	h := append(m.hist[s.DataProduct.ID], pt)
	if len(h) > 180 {
		h = h[len(h)-180:]
	}
	m.hist[s.DataProduct.ID] = h
}

func (m *Memory) HealthHistory(id string) []domain.HealthPoint {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]domain.HealthPoint(nil), m.hist[id]...)
}

func (m *Memory) Snapshot(id string) (domain.Snapshot, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.snaps[id]
	return s, ok
}

func (m *Memory) AllSnapshots() []domain.Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]domain.Snapshot, 0, len(m.snaps))
	for _, s := range m.snaps {
		out = append(out, s)
	}
	return out
}

func (m *Memory) Products() []domain.DataProduct {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]domain.DataProduct, 0, len(m.products))
	for _, p := range m.products {
		out = append(out, p)
	}
	return out
}

func (m *Memory) DAGs(id string) []domain.DAG {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]domain.DAG(nil), m.dags[id]...)
}

func (m *Memory) AllDAGs() []domain.DAG {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []domain.DAG
	for _, dags := range m.dags {
		out = append(out, dags...)
	}
	return out
}

func (m *Memory) Checks(id string) []domain.Check {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]domain.Check(nil), m.checks[id]...)
}

func (m *Memory) AllChecks() []domain.Check {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []domain.Check
	for _, cs := range m.checks {
		out = append(out, cs...)
	}
	return out
}

func (m *Memory) AddSub(s domain.Subscription) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.subs[s.ID] = s
}

func (m *Memory) DeleteSub(id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.subs[id]; !ok {
		return false
	}
	delete(m.subs, id)
	return true
}

func (m *Memory) Subs() []domain.Subscription {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]domain.Subscription, 0, len(m.subs))
	for _, s := range m.subs {
		out = append(out, s)
	}
	return out
}

func (m *Memory) SubsFor(dpID string) []domain.Subscription {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []domain.Subscription
	for _, s := range m.subs {
		if s.DataProductID == dpID {
			out = append(out, s)
		}
	}
	return out
}

func (m *Memory) SetMeta(fn func(*domain.PollMeta)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	fn(&m.meta)
}

func (m *Memory) Meta() domain.PollMeta {
	m.mu.RLock()
	defer m.mu.RUnlock()
	meta := m.meta
	meta.DataProductCount = len(m.snaps)
	var n, dags, checks, hist int
	for id, s := range m.snaps {
		n += m.snapBytes[id]
		dags += len(s.Pipeline)
		checks += len(s.Quality)
	}
	for _, h := range m.hist {
		hist += len(h)
	}
	if n == 0 {
		for _, s := range m.snaps {
			b, _ := json.Marshal(s)
			n += len(b)
		}
	}
	meta.SnapshotBytesEstimate = n
	meta.Scale = estimateScale(scaleInput{
		products:     len(m.snaps),
		dags:         dags,
		checks:       checks,
		histPoints:   hist,
		snapshotJSON: n,
	})
	return meta
}

func (m *Memory) TouchClock(t time.Time) {
	m.SetMeta(func(p *domain.PollMeta) { p.LastClockTick = t.UTC() })
}

func (m *Memory) TouchAstroRuns(t time.Time) {
	m.SetMeta(func(p *domain.PollMeta) { p.LastAstroRunPoll = t.UTC() })
}

func (m *Memory) TouchAstroTags(t time.Time) {
	m.SetMeta(func(p *domain.PollMeta) { p.LastAstroTagPoll = t.UTC() })
}

func (m *Memory) TouchFivetran(t time.Time) {
	m.SetMeta(func(p *domain.PollMeta) { p.LastFivetranPoll = t.UTC() })
}

func (m *Memory) TouchQuality(t time.Time) {
	m.SetMeta(func(p *domain.PollMeta) { p.LastQualityPoll = t.UTC() })
}

func (m *Memory) TouchLineage(t time.Time) {
	m.SetMeta(func(p *domain.PollMeta) { p.LastLineagePoll = t.UTC() })
}
