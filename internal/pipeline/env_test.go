package pipeline

import (
	"testing"

	"github.com/inorbit/inorbit/internal/domain"
)

func TestEnvKind(t *testing.T) {
	cases := map[string]string{
		"prod":                 EnvProduction,
		"production":           EnvProduction,
		"alpha-prod":           EnvProduction,
		"preprod":              EnvPreprod,
		"pre-prod":             EnvPreprod,
		"alpha-preprod":        EnvPreprod,
		"staging":              EnvPreprod,
		"uat":                  EnvPreprod,
		"non-prod":             EnvUnknown,
		"sandbox":              EnvSandbox,
		"dev":                  EnvSandbox,
		"":                     EnvUnknown,
		"analytics-deployment": EnvUnknown,
	}
	for name, want := range cases {
		if got := EnvKind(name); got != want {
			t.Fatalf("%q: got %s want %s", name, got, want)
		}
	}
}

func TestScoreDAGSkipsPreprodWhenProdExists(t *testing.T) {
	dags := []domain.DAG{
		{DAGID: "hourly", DeploymentName: "prod", Status: "SUCCESS"},
		{DAGID: "hourly", DeploymentName: "preprod", Status: "FAILED"},
	}
	hasProd := HasProduction(dags)
	if !hasProd {
		t.Fatal("expected production")
	}
	if !ScoreDAG(dags[0], hasProd) {
		t.Fatal("prod DAG should score")
	}
	if ScoreDAG(dags[1], hasProd) {
		t.Fatal("preprod DAG should not score when prod exists")
	}
}
