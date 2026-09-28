package lineage

import (
	"testing"

	"github.com/inorbit/inorbit/internal/domain"
)

func TestMapJSONArrayAndProductMatch(t *testing.T) {
	rows := []map[string]any{
		{
			"DATA_PRODUCT_ID":          "alpha",
			"DATA_PRODUCT_NAME":        "alpha",
			"STATUS":                   "AT_RISK",
			"HEALTH_SCORE":             54.0,
			"UPSTREAM_SOURCES":         `[{"name":"beta","type":"data_product","status":"TRUSTED","health_score":91},{"name":"FIVETRAN_DB.ORDERS","type":"fivetran_db","status":"UNKNOWN"}]`,
			"DOWNSTREAM_CONSUMERS":     `[{"name":"finance_reports","type":"data_product","status":"CAUTION","health_score":72},{"name":"svc","type":"service_account","status":"N/A"}]`,
			"UPSTREAM_COUNT":           2,
			"DIRECT_DOWNSTREAM_COUNT":  2,
			"DIRECT_DP_CONSUMER_COUNT": 1,
			"SERVICE_ACCOUNT_COUNT":    1,
			"BLAST_RADIUS_COUNT":       12,
			"BLAST_RADIUS_SCORE":       "HIGH IMPACT",
		},
	}
	products := []domain.DataProduct{{ID: "alpha", Name: "alpha"}, {ID: "beta", Name: "beta"}}
	got := Map(rows, products)
	if len(got) != 2 {
		t.Fatalf("len %d", len(got))
	}
	a := got[0]
	if a.DataProductID != "alpha" || len(a.UpstreamSources) != 2 || a.UpstreamSources[0].Name != "beta" {
		t.Fatalf("alpha %+v", a)
	}
	if a.BlastRadiusScore != "HIGH IMPACT" || a.ServiceAccountCount != 1 {
		t.Fatalf("impact %+v", a)
	}
	if got[1].DataProductID != "beta" || len(got[1].UpstreamSources) != 0 {
		t.Fatalf("missing product should be empty %+v", got[1])
	}
}
