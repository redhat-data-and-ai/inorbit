package store

import (
	"testing"
	"time"

	"github.com/inorbit/inorbit/internal/domain"
)

func TestHealthHistoryMergesMartAndLiveToday(t *testing.T) {
	now := time.Date(2026, 10, 5, 15, 0, 0, 0, time.UTC)
	m := New()
	m.SetMartHealth(map[string][]domain.HealthPoint{
		"forecasting": {
			{At: now.AddDate(0, 0, -2), Score: 77, Status: domain.HealthCaution},
			{At: time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC), Score: 78, Status: domain.HealthCaution},
		},
	})
	m.PutSnapshot(domain.Snapshot{
		DataProduct: domain.DataProduct{ID: "forecasting"},
		Health:      domain.HealthStatus{HealthScore: 89.2, Status: domain.HealthTrusted, TotalChecks: 73, FailedChecks: 3},
		UpdatedAt:   now,
	})
	hist := m.HealthHistory("forecasting")
	if len(hist) != 2 {
		t.Fatalf("expected yesterday + live today, got %+v", hist)
	}
	if hist[0].Score != 77 || hist[1].Score != 89.2 {
		t.Fatalf("series %+v", hist)
	}
}

func TestHealthHistorySkipsIncompleteLiveZero(t *testing.T) {
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	m := New()
	m.SetMartHealth(map[string][]domain.HealthPoint{
		"alpha": {{At: now.AddDate(0, 0, -1), Score: 70, Status: domain.HealthCaution}},
	})
	m.PutSnapshot(domain.Snapshot{
		DataProduct: domain.DataProduct{ID: "alpha"},
		Health:      domain.HealthStatus{Status: domain.HealthAtRisk, StatusMessage: "No checks configured"},
		UpdatedAt:   now,
	})
	hist := m.HealthHistory("alpha")
	if len(hist) != 1 || hist[0].Score != 70 {
		t.Fatalf("incomplete live zero should not append %+v", hist)
	}
}
