package store

import (
	"testing"

	"github.com/inorbit/inorbit/internal/domain"
)

func TestEstimateScaleLinearTo200(t *testing.T) {
	got := estimateScale(scaleInput{
		products:     4,
		dags:         80,
		checks:       200,
		histPoints:   40,
		snapshotJSON: 400 << 10,
	})
	if got.TargetDataProducts != domain.ScaleTargetDataProducts {
		t.Fatalf("target %d", got.TargetDataProducts)
	}
	if got.BytesPerProduct != 100<<10 {
		t.Fatalf("per product %d", got.BytesPerProduct)
	}
	wantJSON := (100 << 10) * domain.ScaleTargetDataProducts
	if got.EstimatedSnapshotJSONBytesAtTarget != wantJSON {
		t.Fatalf("json at 200: %d", got.EstimatedSnapshotJSONBytesAtTarget)
	}
	if got.EstimatedRSSBytesAtTarget < processBaselineBytes+wantJSON*3 {
		t.Fatalf("rss too small: %d", got.EstimatedRSSBytesAtTarget)
	}
	if got.EstimatedRSSBytesAtTarget > 512<<20 {
		t.Fatalf("rss implausibly large for 100KiB/product: %d", got.EstimatedRSSBytesAtTarget)
	}
}
