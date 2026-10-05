package healthsnap

import (
	"testing"
	"time"

	"github.com/inorbit/inorbit/internal/domain"
)

func TestMapGroupsDailyPointsAndDedupe(t *testing.T) {
	products := []domain.DataProduct{{ID: "forecasting", Name: "forecasting"}}
	day := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	rows := []map[string]any{
		{"DATA_PRODUCT_ID": "forecasting", "SNAPSHOT_DATE": day, "HEALTH_SCORE": 77.9, "STATUS": "CAUTION", "TOTAL_CHECKS": 71, "FAILED_CHECKS": 14},
		{"DATA_PRODUCT_ID": "forecasting", "SNAPSHOT_DATE": day.Add(12 * time.Hour), "HEALTH_SCORE": 78.1, "STATUS": "CAUTION"},
		{"DATA_PRODUCT_ID": "forecasting", "SNAPSHOT_DATE": day.AddDate(0, 0, 1), "HEALTH_SCORE": 89.2, "STATUS": "TRUSTED"},
		{"DATA_PRODUCT_ID": "other", "SNAPSHOT_DATE": day, "HEALTH_SCORE": 100.0, "STATUS": "TRUSTED"},
	}
	got := Map(rows, products)
	pts := got["forecasting"]
	if len(pts) != 2 {
		t.Fatalf("points %+v", pts)
	}
	if pts[0].Score != 78.1 || pts[0].Status != domain.HealthCaution {
		t.Fatalf("latest same-day row should win %+v", pts[0])
	}
	if pts[1].Score != 89.2 || pts[1].Status != domain.HealthTrusted {
		t.Fatalf("second day %+v", pts[1])
	}
}

func TestParseStatusFromScore(t *testing.T) {
	if parseStatus("", 91) != domain.HealthTrusted {
		t.Fatal("trusted")
	}
	if parseStatus("at risk", 40) != domain.HealthAtRisk {
		t.Fatal("at risk")
	}
}
