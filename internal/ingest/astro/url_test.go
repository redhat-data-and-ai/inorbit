package astro_test

import (
	"testing"

	"github.com/inorbit/inorbit/internal/ingest/astro"
)

func TestAirflowAPIBasesInsertsDeploymentID(t *testing.T) {
	bases := astro.AirflowAPIBases(astro.Deployment{
		ID:            "dep123",
		AirflowAPIURL: "https://astro.example.com/api/v1",
	})
	wantV1 := "https://astro.example.com/dep123/api/v1"
	wantV2 := "https://astro.example.com/dep123/api/v2"
	foundV1, foundV2 := false, false
	for _, b := range bases {
		if b == wantV1 {
			foundV1 = true
		}
		if b == wantV2 {
			foundV2 = true
		}
	}
	if !foundV1 || !foundV2 {
		t.Fatalf("missing %s or %s in %v", wantV1, wantV2, bases)
	}
}

func TestAirflowAPIBasesPrefersV2OnDeploymentRoot(t *testing.T) {
	bases := astro.AirflowAPIBases(astro.Deployment{
		ID:            "dep123",
		AirflowAPIURL: "https://astro.example.com/dep123",
	})
	want := "https://astro.example.com/dep123/api/v2"
	if len(bases) == 0 || bases[0] != want {
		t.Fatalf("want first %s, got %v", want, bases)
	}
}

func TestGridURLMatchesMartShape(t *testing.T) {
	got := astro.GridURL(astro.Deployment{
		ID:     "dep123",
		OrgURL: "https://astro.example.com",
	}, "catalog_hourly")
	want := "https://astro.example.com/dep123/dags/catalog_hourly"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestGridURLUsesOriginFromAPIWhenOrgMissing(t *testing.T) {
	got := astro.GridURL(astro.Deployment{
		ID:            "dep123",
		AirflowAPIURL: "https://astro.example.com/dep123/api/v2",
	}, "orders_daily")
	want := "https://astro.example.com/dep123/dags/orders_daily"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestGridURLPlaceholderWhenDeploymentIDMissing(t *testing.T) {
	got := astro.GridURL(astro.Deployment{
		OrgURL: "https://astro.example.com",
	}, "some_dag")
	want := "https://astro.example.com/:deployment_id/dags/some_dag"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestGridURLEmptyWithoutDAG(t *testing.T) {
	if astro.GridURL(astro.Deployment{OrgURL: "https://astro.example.com", ID: "dep"}, "  ") != "" {
		t.Fatal("expected empty URL without dag_id")
	}
}
