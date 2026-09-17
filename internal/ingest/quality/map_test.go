package quality_test

import (
	"testing"

	"github.com/inorbit/inorbit/internal/domain"
	"github.com/inorbit/inorbit/internal/ingest/quality"
)

func TestMapStatusAndDimension(t *testing.T) {
	if quality.MapStatus("pass") != domain.CheckPassed {
		t.Fatal("pass")
	}
	if quality.MapStatus("FAIL") != domain.CheckFailed {
		t.Fatal("fail")
	}
	if quality.MapStatus("skipped") != "" {
		t.Fatal("skip")
	}
	if quality.MapDimension("TIMELINESS", "", "") != domain.DimFreshness {
		t.Fatal("timeliness")
	}
	if quality.MapDimension("", "tag:completeness", "") != domain.DimCompleteness {
		t.Fatal("tag")
	}
	if quality.MapDimension("", "", "not_null_id") != domain.DimCompleteness {
		t.Fatal("name")
	}
	if quality.MapDimension("OTHER", "", "misc") != domain.DimUnknown {
		t.Fatal("unknown")
	}
}

func TestDBTCheckAccuracyFallback(t *testing.T) {
	dp := domain.DataProduct{ID: "orders", Name: "orders"}
	c, ok := quality.DBTCheck(dp, map[string]any{
		"TEST_NAME": "assert_custom", "STATUS": "pass", "TEST_UNIQUE_ID": "test.x",
	})
	if !ok || c.Dimension != domain.DimAccuracy {
		t.Fatalf("%+v ok=%v", c, ok)
	}
}

func TestValidationCheckLatestRun(t *testing.T) {
	dp := domain.DataProduct{ID: "catalog", Name: "catalog"}
	c, ok := quality.ValidationCheck(dp, map[string]any{
		"NAME": "row_count", "STATUS": "PASS", "SEVERITY": "HIGH",
		"DIMENSION": "COMPLETENESS", "RUN_ID": "r1",
	})
	if !ok || c.Status != domain.CheckPassed || c.Dimension != domain.DimCompleteness {
		t.Fatalf("%+v ok=%v", c, ok)
	}
	fallback, ok := quality.ValidationCheck(dp, map[string]any{
		"STATUS": "FAIL", "TABLE_NAME": "MART.FACT", "ELEMENT": "qty", "RUN_ID": "r2",
	})
	if !ok || fallback.Name != "MART.FACT qty" {
		t.Fatalf("fallback name %+v ok=%v", fallback, ok)
	}
	rows := []map[string]any{
		{"RUN_ID": "old", "RUN_TIME": "2026-01-01T00:00:00Z", "NAME": "a"},
		{"RUN_ID": "new", "RUN_TIME": "2026-09-01T00:00:00Z", "NAME": "b"},
		{"RUN_ID": "new", "RUN_TIME": "2026-09-01T00:00:01Z", "NAME": "c"},
	}
	got := quality.KeepLatestRun(rows, []string{"RUN_TIME"}, []string{"RUN_ID"})
	if len(got) != 2 {
		t.Fatalf("got %d", len(got))
	}
}
