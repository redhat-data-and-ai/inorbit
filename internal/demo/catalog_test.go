package demo_test

import (
	"testing"
	"time"

	"github.com/inorbit/inorbit/internal/demo"
	"github.com/inorbit/inorbit/internal/domain"
)

func TestLoadReadsCatalogJSON(t *testing.T) {
	now := time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)
	products, dags, checks, err := demo.Load(now)
	if err != nil {
		t.Fatal(err)
	}
	if len(products) < 2 {
		t.Fatalf("catalog data_products: %+v", products)
	}
	if len(dags) < 2 || len(checks) < 1 {
		t.Fatalf("catalog snapshot dags=%d checks=%d", len(dags), len(checks))
	}
	byID := map[string]domain.DataProduct{}
	for _, p := range products {
		if p.ID == "" {
			t.Fatal("product missing id")
		}
		byID[p.ID] = p
	}
	for _, d := range dags {
		if _, ok := byID[d.DataProductID]; !ok {
			t.Fatalf("dag %s references unknown product %s", d.DAGID, d.DataProductID)
		}
	}
	primary := dags[0]
	evRed := false
	for _, d := range dags {
		if !d.IsPrimary || d.CompletedAt == nil || d.IntervalMins == nil || d.SLAMinutes == nil {
			continue
		}
		age := now.Sub(*d.CompletedAt).Minutes()
		if age > *d.IntervalMins+*d.SLAMinutes {
			evRed = true
			primary = d
			break
		}
	}
	if !evRed {
		t.Fatalf("demo catalog should include one overdue primary DAG, got %+v", dags)
	}
	if primary.AstroURL == "" {
		t.Fatal("demo DAG missing astro_url")
	}
}
