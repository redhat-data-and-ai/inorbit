package quality

import (
	"strings"
	"testing"

	"github.com/inorbit/inorbit/internal/domain"
)

func TestValidationLatestSQLSkipsQueryText(t *testing.T) {
	have := map[string]bool{
		"RUN_ID": true, "RUN_TIME": true, "STATUS": true, "NAME": true,
		"DIMENSION": true, "QUERY": true, "EXPECTED_OUTPUT": true,
	}
	q, ok := validationLatestSQL("ANALYTICS.QUALITY.VALIDATION_RESULT", have)
	if !ok {
		t.Fatal("expected sql")
	}
	upper := strings.ToUpper(q)
	if strings.Contains(upper, "QUERY") || strings.Contains(upper, "SELECT *") || strings.Contains(upper, "EXPECTED_OUTPUT") {
		t.Fatalf("projected query text: %s", q)
	}
	if !strings.Contains(upper, "DIMENSION") || !strings.Contains(upper, "FIRST_VALUE(RUN_ID)") {
		t.Fatalf("sql: %s", q)
	}
}

func TestElementaryLatestSQLWithoutDimensionColumn(t *testing.T) {
	have := map[string]bool{
		"INVOCATION_ID": true, "DETECTED_AT": true, "STATUS": true,
		"TEST_NAME": true, "TEST_UNIQUE_ID": true, "TEST_RESULTS_QUERY": true,
	}
	q, ok := elementaryLatestSQL("ANALYTICS.ELEMENTARY.ELEMENTARY_TEST_RESULTS", have, "", nil)
	if !ok {
		t.Fatal("expected sql")
	}
	upper := strings.ToUpper(q)
	if strings.Contains(upper, "DIMENSION") || strings.Contains(upper, "TEST_EXECUTION_ID") || strings.Contains(upper, "TEST_RESULTS_QUERY") {
		t.Fatalf("sql referenced a missing column: %s", q)
	}
	if !strings.Contains(upper, "TEST_NAME") || !strings.Contains(upper, "FIRST_VALUE(E.INVOCATION_ID)") {
		t.Fatalf("sql: %s", q)
	}
}

func TestElementaryLatestSQLJoinsQualityDimension(t *testing.T) {
	have := map[string]bool{
		"INVOCATION_ID": true, "DETECTED_AT": true, "STATUS": true,
		"TEST_NAME": true, "TEST_UNIQUE_ID": true,
	}
	tests := map[string]bool{"UNIQUE_ID": true, "QUALITY_DIMENSION": true, "GENERATED_AT": true}
	q, ok := elementaryLatestSQL(
		"ANALYTICS.ELEMENTARY.ELEMENTARY_TEST_RESULTS",
		have,
		"ANALYTICS.ELEMENTARY.DBT_TESTS",
		tests,
	)
	if !ok {
		t.Fatal("expected sql")
	}
	upper := strings.ToUpper(q)
	if !strings.Contains(upper, "LEFT JOIN (SELECT UNIQUE_ID, NULLIF(TRIM(QUALITY_DIMENSION), '') AS DIMENSION FROM ANALYTICS.ELEMENTARY.DBT_TESTS") || !strings.Contains(upper, "T.DIMENSION AS DIMENSION") || !strings.Contains(upper, "T.UNIQUE_ID = E.TEST_UNIQUE_ID") {
		t.Fatalf("sql: %s", q)
	}
	if strings.Contains(upper, "TRY_PARSE_JSON") {
		t.Fatalf("meta was not on the catalog: %s", q)
	}
}

func TestElementaryLatestSQLReadsDimensionFromMeta(t *testing.T) {
	have := map[string]bool{
		"INVOCATION_ID": true, "DETECTED_AT": true, "STATUS": true,
		"TEST_NAME": true, "TEST_UNIQUE_ID": true,
	}
	tests := map[string]bool{"UNIQUE_ID": true, "META": true}
	q, ok := elementaryLatestSQL(
		"ANALYTICS.ELEMENTARY.ELEMENTARY_TEST_RESULTS",
		have,
		"ANALYTICS.ELEMENTARY.DBT_TESTS",
		tests,
	)
	if !ok {
		t.Fatal("expected sql")
	}
	upper := strings.ToUpper(q)
	if !strings.Contains(upper, "TRY_PARSE_JSON(TO_VARCHAR(META)):DIMENSION") || !strings.Contains(upper, "TRY_PARSE_JSON(TO_VARCHAR(META)):QUALITY_DIMENSION") {
		t.Fatalf("sql: %s", q)
	}
}

func TestDimensionFromNameMatchesDBT(t *testing.T) {
	cases := []struct {
		name string
		want domain.Dimension
	}{
		{"not_null_orders_id", domain.DimCompleteness},
		{"unique_order_id", domain.DimUniqueness},
		{"accepted_values_status", domain.DimValidity},
		{"relationships_orders", domain.DimConsistency},
		{"recency_orders", domain.DimFreshness},
		{"contract_schema", domain.DimValidity},
		{"calculate_margin", domain.DimValidity},
		{"lock_due_date", domain.DimConsistency},
		{"filter_region", domain.DimAccuracy},
		{"split_amount", domain.DimAccuracy},
		{"reconcile_totals", domain.DimAccuracy},
		{"sla_daily", domain.DimFreshness},
		{"check_row_count", domain.DimValidity},
	}
	for _, tc := range cases {
		if got := dimensionFromName(tc.name); got != tc.want {
			t.Fatalf("%s: got %s want %s", tc.name, got, tc.want)
		}
	}
}
