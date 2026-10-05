package fivetran

import (
	"testing"

	"github.com/inorbit/inorbit/internal/domain"
)

func TestEnrichUpstreamGroupsBySchemaAndType(t *testing.T) {
	nodes := []domain.LineageNode{
		{Name: "SNOWPIPE_DB.RAW", Type: "snowpipe_db", Status: "UNKNOWN"},
		{Name: "EXT.TABLE", Type: "external_table", Status: "UNKNOWN"},
		{Name: "FIVETRAN_DB.ORDERS", Type: "fivetran_db", Status: "UNKNOWN"},
	}
	conns := []domain.Connector{
		{Schema: "orders", Service: "google_sheets", Status: "SUCCESS"},
		{Schema: "orders", Service: "google_sheets", Status: "SUCCESS", Paused: true},
		{Schema: "orders", Service: "s3", Status: "SUCCESS"},
		{Schema: "orders", Service: "salesforce", Status: "SUCCESS"},
		{Schema: "orders", Service: "postgres", Status: "SUCCESS", Paused: true},
	}
	got := EnrichUpstream(nodes, conns)
	if len(got) != 6 {
		t.Fatalf("len %d %+v", len(got), got)
	}
	if got[0].Type != "snowpipe_db" || got[0].ConnectorService != "" {
		t.Fatalf("snowpipe %+v", got[0])
	}
	if got[1].Type != "external_table" {
		t.Fatalf("external %+v", got[1])
	}
	byType := map[string]domain.LineageNode{}
	for _, n := range got[2:] {
		byType[n.ConnectorService] = n
	}
	sheets := byType["google_sheets"]
	if sheets.ConnectorType != "Google Sheets" || sheets.ConnectionCount != 2 || sheets.PausedCount != 1 {
		t.Fatalf("sheets %+v", sheets)
	}
	if byType["s3"].PausedCount != 0 || byType["s3"].ConnectionCount != 1 {
		t.Fatalf("s3 %+v", byType["s3"])
	}
	if byType["postgres"].PausedCount != 1 {
		t.Fatalf("postgres %+v", byType["postgres"])
	}
	if sheets.Name != "orders" {
		t.Fatalf("generic fivetran_db node should title from schema, got %q", sheets.Name)
	}
}

func TestEnrichKeepsMartSchemaName(t *testing.T) {
	nodes := []domain.LineageNode{
		{Name: "orders_design", Type: "fivetran_db"},
		{Name: "orders_security", Type: "fivetran_db"},
	}
	conns := []domain.Connector{
		{Schema: "orders_design", Service: "google_sheets", Status: "SUCCESS"},
		{Schema: "orders_security", Service: "google_sheets", Status: "SUCCESS", Paused: true},
	}
	got := EnrichUpstream(nodes, conns)
	if len(got) != 2 || got[0].Name != "orders_design" || got[1].Name != "orders_security" {
		t.Fatalf("%+v", got)
	}
	if got[1].PausedCount != 1 || got[1].ConnectorType != "Google Sheets" {
		t.Fatalf("paused %+v", got[1])
	}
}

func TestServiceNameKnownAndFallback(t *testing.T) {
	if ServiceName("google_sheets") != "Google Sheets" {
		t.Fatal(ServiceName("google_sheets"))
	}
	if ServiceName("custom_api") != "Custom Api" {
		t.Fatal(ServiceName("custom_api"))
	}
}
