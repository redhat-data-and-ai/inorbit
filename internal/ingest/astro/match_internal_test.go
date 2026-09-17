package astro

import (
	"encoding/json"
	"testing"

	"github.com/inorbit/inorbit/internal/domain"
)

func TestParseTagNamesFormats(t *testing.T) {
	cases := []struct {
		raw  string
		want []string
	}{
		{`[{"name":"catalog"},{"name":"is_primary_dag"}]`, []string{"catalog", "is_primary_dag"}},
		{`["catalog","is_custom_dag"]`, []string{"catalog", "is_custom_dag"}},
		{`"catalog, is_custom_dag"`, []string{"catalog", "is_custom_dag"}},
		{`null`, nil},
	}
	for _, tc := range cases {
		got := parseTagNames(json.RawMessage(tc.raw))
		if len(got) != len(tc.want) {
			t.Fatalf("%s: got %v want %v", tc.raw, got, tc.want)
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Fatalf("%s: got %v want %v", tc.raw, got, tc.want)
			}
		}
	}
}

func TestMatchDAGUsesFirstProductTagEvenWhenNameDoesNotMatch(t *testing.T) {
	products := []domain.DataProduct{{ID: "catalog", Name: "catalog"}}
	got, ok := MatchDAG("legacy_load", []string{"is_custom_dag", "catalog"}, "", products, nil)
	if !ok || got.ID != "catalog" {
		t.Fatalf("got %+v ok=%v", got, ok)
	}
}

func TestMatchDAGNormalizesHyphenAndUnderscoreTags(t *testing.T) {
	products := []domain.DataProduct{
		{ID: "metricscore", Name: "metricscore"},
		{ID: "inventory", Name: "inventory"},
	}
	got, ok := MatchDAG("dbt_metricscore_daily", []string{"ops-logs", "metrics-core"}, "", products, nil)
	if !ok || got.ID != "metricscore" {
		t.Fatalf("hyphen tag: %+v ok=%v", got, ok)
	}
	got, ok = MatchDAG("dbt_inventory_daily", []string{"inventory"}, "", products, nil)
	if !ok || got.ID != "inventory" {
		t.Fatalf("exact tag: %+v ok=%v", got, ok)
	}
	got, ok = MatchDAG("dbt_metricscore_30m", nil, "", products, nil)
	if !ok || got.ID != "metricscore" {
		t.Fatalf("dag_id token: %+v ok=%v", got, ok)
	}
}

func TestMatchDAGDoesNotAttachBareDaily(t *testing.T) {
	products := []domain.DataProduct{{ID: "inorbit", Name: "inorbit"}}
	if _, ok := MatchDAG("daily", nil, "", products, nil); ok {
		t.Fatal("bare daily should not match inorbit")
	}
	got, ok := MatchDAG("dbt_inorbit_daily", nil, "", products, nil)
	if !ok || got.ID != "inorbit" {
		t.Fatalf("dbt_inorbit_daily: %+v ok=%v", got, ok)
	}
}
