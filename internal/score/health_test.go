package score_test

import (
	"testing"
	"time"

	"github.com/inorbit/inorbit/internal/domain"
	"github.com/inorbit/inorbit/internal/score"
)

func TestBasePointsAndFailedOnlyDeduction(t *testing.T) {
	cfg := score.DefaultConfig()
	c := score.Apply(domain.Check{
		Severity: domain.SevCritical, Status: domain.CheckFailed, BlastRadiusMultiplier: 1,
	}, time.Now(), cfg)
	if c.BasePoints != 10 || c.FinalDeduction != 10 {
		t.Fatalf("critical fail: %+v", c)
	}
	pass := score.Apply(domain.Check{
		Severity: domain.SevCritical, Status: domain.CheckPassed, IsFlapping: true,
	}, time.Now(), cfg)
	if pass.BasePoints != 3 || pass.FinalDeduction != 0 {
		t.Fatalf("flapping pass should not deduct (matches dbt SQL): %+v", pass)
	}
}

func TestTimeDecayCritical(t *testing.T) {
	cfg := score.DefaultConfig()
	fail := time.Now().UTC().Add(-3 * time.Hour)
	c := score.Apply(domain.Check{
		Severity: domain.SevCritical, Status: domain.CheckFailed, FirstFailedAt: &fail,
		BlastRadiusMultiplier: 1,
	}, time.Now().UTC(), cfg)
	if c.TimeDecayMultiplier != 2 {
		t.Fatalf("want 2.0 decay, got %v", c.TimeDecayMultiplier)
	}
}

func TestCoverageCapTwoDimensions(t *testing.T) {
	cfg := score.DefaultConfig()
	now := time.Now().UTC()
	checks := []domain.Check{
		score.Apply(domain.Check{
			DataProductID: "p1", Name: "a", Dimension: domain.DimAccuracy,
			Severity: domain.SevLow, Status: domain.CheckPassed, ExecutedAt: now,
		}, now, cfg),
		score.Apply(domain.Check{
			DataProductID: "p1", Name: "f", Dimension: domain.DimFreshness,
			Severity: domain.SevCritical, Status: domain.CheckFailed, ExecutedAt: now,
			BlastRadiusMultiplier: 1,
		}, now, cfg),
	}
	h := score.Evaluate([]domain.DataProduct{{ID: "p1", Name: "p1"}}, checks, cfg)[0]
	// raw is 95 (FRESHNESS 90, ACCURACY 100). ≤2 dims caps at 55.
	// Perfect 100 bypasses caps (same as int_health_score.sql).
	if !h.CoverageCapApplied || h.HealthScore != 55 {
		t.Fatalf("2-dim imperfect score should cap at 55: %+v", h)
	}
	if h.Status != domain.HealthAtRisk {
		t.Fatalf("55 is AT_RISK, got %s", h.Status)
	}
}

func TestNoChecksAtRisk(t *testing.T) {
	h := score.Evaluate([]domain.DataProduct{{ID: "x", Name: "x"}}, nil, score.DefaultConfig())[0]
	if h.Status != domain.HealthAtRisk || h.HealthScore != 0 {
		t.Fatalf("%+v", h)
	}
}
