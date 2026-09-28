package quality_test

import (
	"testing"
	"time"

	"github.com/inorbit/inorbit/internal/domain"
	"github.com/inorbit/inorbit/internal/engine"
	"github.com/inorbit/inorbit/internal/ingest/quality"
	"github.com/inorbit/inorbit/internal/store"
)

func TestApplyStoresMissingValidation(t *testing.T) {
	st := store.New()
	eng := engine.New(st)
	now := time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)
	st.UpsertProduct(domain.DataProduct{ID: "gamma", Name: "gamma"})
	quality.Apply(st, eng, nil, map[string]domain.QualitySources{
		"gamma": {
			Validation: domain.QualitySource{Status: domain.QualityMissing, Relation: "ALPHA_DB.QUALITY.VALIDATION_RESULT"},
			DBT:        domain.QualitySource{Status: domain.QualityDisabled},
		},
	}, now)
	snap, ok := st.Snapshot("gamma")
	if !ok {
		t.Fatal("missing snapshot")
	}
	if snap.QualitySources.Validation.Status != domain.QualityMissing {
		t.Fatalf("validation status %q", snap.QualitySources.Validation.Status)
	}
	if len(snap.Quality) != 0 {
		t.Fatalf("expected no checks, got %d", len(snap.Quality))
	}
}
