package alert

import (
	"log"
	"strconv"
	"time"

	"github.com/inorbit/inorbit/internal/domain"
	"github.com/inorbit/inorbit/internal/store"
)

type incident struct {
	Key        string
	OpenedAt   time.Time
	LastSentAt time.Time
	Summary    string
}

type Router struct {
	Store    *store.Memory
	Log      *log.Logger
	open     map[string]incident
	remindAfter time.Duration
}

func New(st *store.Memory, lg *log.Logger) *Router {
	return &Router{
		Store:       st,
		Log:         lg,
		open:        map[string]incident{},
		remindAfter: 24 * time.Hour,
	}
}

// Scan emits subscribe-only alerts from the current snapshot. Local default is log.
func (r *Router) Scan(now time.Time) []string {
	now = now.UTC()
	var sent []string
	active := map[string]struct{}{}
	for _, snap := range r.Store.AllSnapshots() {
		facts := factsFor(snap)
		for _, f := range facts {
			active[f.key] = struct{}{}
			for _, sub := range r.Store.SubsFor(snap.DataProduct.ID) {
				if !matches(sub.Audience, f.kind) {
					continue
				}
				msg := f.summary + " → " + sub.SlackChannel + " (" + string(sub.Audience) + ")"
				prev, exists := r.open[f.key]
				if !exists {
					r.open[f.key] = incident{Key: f.key, OpenedAt: now, LastSentAt: now, Summary: f.summary}
					r.logf("ALERT open: %s", msg)
					sent = append(sent, msg)
					continue
				}
				if now.Sub(prev.LastSentAt) >= r.remindAfter {
					prev.LastSentAt = now
					r.open[f.key] = prev
					r.logf("ALERT remind: %s", msg)
					sent = append(sent, msg)
				}
			}
		}
	}
	for k, inc := range r.open {
		if _, ok := active[k]; ok {
			continue
		}
		delete(r.open, k)
		r.logf("ALERT resolved: %s (%s)", k, inc.Summary)
		sent = append(sent, "resolved:"+k)
	}
	return sent
}

type fact struct {
	key     string
	kind    string
	summary string
}

func factsFor(s domain.Snapshot) []fact {
	var out []fact
	if s.Health.Status == domain.HealthAtRisk {
		out = append(out, fact{
			key:     s.DataProduct.ID + ":health",
			kind:    "business",
			summary: s.DataProduct.Name + " health " + string(s.Health.Status) + " score " + trim(s.Health.HealthScore),
		})
	}
	if s.Freshness.FreshnessStatus == domain.FreshnessRed {
		out = append(out, fact{
			key:     s.DataProduct.ID + ":freshness",
			kind:    "business",
			summary: s.DataProduct.Name + " freshness RED",
		})
	}
	for _, p := range s.Pipeline {
		if p.DAGOverallStatus == domain.OverallFailed || p.DAGOverallStatus == domain.OverallDelayed {
			out = append(out, fact{
				key:     s.DataProduct.ID + ":dag:" + p.DAGID,
				kind:    "developer",
				summary: p.DAGID + " " + string(p.DAGOverallStatus),
			})
		}
	}
	return out
}

func matches(aud domain.Audience, kind string) bool {
	if aud == domain.AudienceBusiness {
		return kind == "business"
	}
	return true
}

func trim(v float64) string {
	return strconv.FormatFloat(v, 'f', 2, 64)
}

func (r *Router) logf(format string, args ...any) {
	if r.Log != nil {
		r.Log.Printf(format, args...)
		return
	}
	log.Printf(format, args...)
}
