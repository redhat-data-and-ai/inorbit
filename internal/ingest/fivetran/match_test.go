package fivetran

import (
	"testing"

	"github.com/inorbit/inorbit/internal/domain"
)

func TestMatchSchemaToSourceAligned(t *testing.T) {
	products := []domain.DataProduct{
		{ID: "alpha", Name: "alpha", Type: "aggregate"},
		{ID: "beta", Name: "beta", Type: "source-aligned"},
	}
	got, ok := Match("conn-1", "beta", "postgres", products, nil)
	if !ok || got.ID != "beta" {
		t.Fatalf("got %+v ok=%v", got, ok)
	}
}

func TestMatchSkipsAggregateWithSameName(t *testing.T) {
	products := []domain.DataProduct{
		{ID: "beta", Name: "beta", Type: "aggregate"},
	}
	if _, ok := Match("c1", "beta", "postgres", products, nil); ok {
		t.Fatal("aggregate should not auto-match")
	}
}

func TestMatchExplicitIDOnAnyType(t *testing.T) {
	products := []domain.DataProduct{
		{ID: "payments", Name: "payments", Type: "aggregate"},
	}
	got, ok := Match("abc123", "other_schema", "google_sheets", products, map[string]string{"abc123": "payments"})
	if !ok || got.ID != "payments" {
		t.Fatalf("got %+v ok=%v", got, ok)
	}
}

func TestMatchSchemaToken(t *testing.T) {
	products := []domain.DataProduct{
		{ID: "sheets", Name: "sheets", Type: "source-aligned"},
	}
	got, ok := Match("c1", "sheets.table", "google_sheets", products, nil)
	if !ok || got.ID != "sheets" {
		t.Fatalf("got %+v ok=%v", got, ok)
	}
}

func TestMatchPrefersLongerProduct(t *testing.T) {
	products := []domain.DataProduct{
		{ID: "orders", Name: "orders", Type: "source-aligned"},
		{ID: "orders_design", Name: "orders_design", Type: "source-aligned"},
	}
	got, ok := Match("c1", "orders_design", "google_sheets", products, nil)
	if !ok || got.ID != "orders_design" {
		t.Fatalf("got %+v ok=%v", got, ok)
	}
}
