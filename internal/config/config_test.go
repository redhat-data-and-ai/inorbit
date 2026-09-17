package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/inorbit/inorbit/internal/config"
	"github.com/inorbit/inorbit/internal/domain"
)

func TestLoadExpandsEnvAndSkipsEmptyURLs(t *testing.T) {
	t.Setenv("ASTRO_API_TOKEN", "tok")
	t.Setenv("ASTRO_ORG_URL", "")
	t.Setenv("INORBIT_ASTRO_ORG_URL", "")
	t.Setenv("INORBIT_AIRFLOW_API_URL", "https://airflow.example.com/api/v1")
	t.Setenv("INORBIT_ASTRO_DEPLOYMENT_ID", "abc")
	path := filepath.Join(t.TempDir(), "live.json")
	body := `{
  "astro": {
    "token_env": "ASTRO_API_TOKEN",
    "deployments": [
      {"name": "prod", "id": "${INORBIT_ASTRO_DEPLOYMENT_ID}", "airflow_api_url": "${INORBIT_AIRFLOW_API_URL}"},
      {"name": "empty", "id": "x", "airflow_api_url": ""}
    ]
  },
  "data_products": [
    {"id": "alpha", "name": "alpha", "dag_ids": ["alpha_hourly"]},
    {"id": "beta", "name": "beta"}
  ]
}`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Token() != "tok" {
		t.Fatalf("token %q", cfg.Token())
	}
	deps := cfg.Deployments()
	if len(deps) != 1 || deps[0].ID != "abc" {
		t.Fatalf("deployments %+v", deps)
	}
	if len(cfg.Products()) != 2 {
		t.Fatalf("products %+v", cfg.Products())
	}
	if cfg.DAGMap()["alpha_hourly"] != "alpha" {
		t.Fatalf("dag map %+v", cfg.DAGMap())
	}
	var alphaDB string
	for _, p := range cfg.Products() {
		if p.ID == "alpha" {
			alphaDB = p.ValidXDB
		}
	}
	if alphaDB != "ALPHA_DB" {
		t.Fatalf("validx db %q", alphaDB)
	}
	for _, p := range cfg.Products() {
		if p.ID == "alpha" {
			if !p.ValidX.Enabled || p.ValidX.Schema != "DATATRUST_MARTS" || p.ValidX.Table != "VALIDATION_RESULT" {
				t.Fatalf("alpha validx %+v", p.ValidX)
			}
			if p.DBTLogs.Enabled {
				t.Fatalf("dbt should default off, got %+v", p.DBTLogs)
			}
		}
	}
}

func TestDeploymentsFromOrgURLAndID(t *testing.T) {
	t.Setenv("ASTRO_TOKEN", "tok")
	t.Setenv("ASTRO_ORG_URL", "https://org.example.com")
	t.Setenv("INORBIT_ASTRO_DEPLOYMENT_ID", "dep123")
	t.Setenv("INORBIT_AIRFLOW_API_URL", "")
	path := filepath.Join(t.TempDir(), "live.json")
	body := `{
  "astro": {
    "token_env": "ASTRO_TOKEN",
    "org_url": "${ASTRO_ORG_URL}",
    "deployments": [
      {"name": "prod", "id": "${INORBIT_ASTRO_DEPLOYMENT_ID}", "airflow_api_url": "${INORBIT_AIRFLOW_API_URL}"}
    ]
  },
  "data_products": [{"id": "alpha", "name": "alpha"}]
}`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Token() != "tok" {
		t.Fatalf("token %q", cfg.Token())
	}
	deps := cfg.Deployments()
	if len(deps) != 1 {
		t.Fatalf("deployments %+v", deps)
	}
	if deps[0].ID != "dep123" || deps[0].AirflowAPIURL != "https://org.example.com/dep123" || deps[0].OrgURL != "https://org.example.com" {
		t.Fatalf("deployment %+v", deps[0])
	}
}

func TestTokenFallsBackToASTROAPIToken(t *testing.T) {
	t.Setenv("ASTRO_TOKEN", "")
	t.Setenv("ASTRO_API_TOKEN", "legacy")
	path := filepath.Join(t.TempDir(), "live.json")
	if err := os.WriteFile(path, []byte(`{"astro":{"deployments":[]},"data_products":[]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Astro.TokenEnv != "ASTRO_TOKEN" {
		t.Fatalf("token_env %q", cfg.Astro.TokenEnv)
	}
	if cfg.Token() != "legacy" {
		t.Fatalf("token %q", cfg.Token())
	}
}

func TestQualityTablesAreConfigDriven(t *testing.T) {
	path := filepath.Join(t.TempDir(), "live.json")
	body := `{
  "astro": {"deployments": []},
  "snowflake": {"account": "acct", "role": "reader", "warehouse": "xs_wh"},
  "data_products": [
    {
      "id": "alpha",
      "quality": {
        "validx": {"enabled": true, "database": "ALPHA_DB", "schema": "DATATRUST_MARTS", "table": "VALIDATION_RESULT"},
        "dbt": {"enabled": false, "database": "ALPHA_DB", "schema": "DBTLOGS", "table": "ELEMENTARY_TEST_RESULTS"}
      }
    },
    {
      "id": "beta",
      "quality": {
        "validx": {"enabled": true, "database": "BETA_DB"},
        "dbt": {"enabled": true, "database": "BETA_DB", "schema": "DBTLOGS", "table": "ELEMENTARY_TEST_RESULTS"}
      }
    }
  ]
}`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]domain.DataProduct{}
	for _, p := range cfg.Products() {
		got[p.ID] = p
	}
	fc := got["alpha"]
	if !fc.ValidX.Enabled || fc.ValidX.Database != "ALPHA_DB" || fc.ValidX.Schema != "DATATRUST_MARTS" || fc.ValidX.Table != "VALIDATION_RESULT" {
		t.Fatalf("alpha validx %+v", fc.ValidX)
	}
	if fc.DBTLogs.Enabled {
		t.Fatalf("alpha dbt should be off: %+v", fc.DBTLogs)
	}
	bm := got["beta"]
	if !bm.ValidX.Enabled || bm.ValidX.Database != "BETA_DB" || bm.ValidX.Schema != "DATATRUST_MARTS" {
		t.Fatalf("beta validx %+v", bm.ValidX)
	}
	if !bm.DBTLogs.Enabled || bm.DBTLogs.Database != "BETA_DB" || bm.DBTLogs.Schema != "DBTLOGS" || bm.DBTLogs.Table != "ELEMENTARY_TEST_RESULTS" {
		t.Fatalf("beta dbt %+v", bm.DBTLogs)
	}
	t.Setenv("SNOWFLAKE_ACCOUNT", "")
	t.Setenv("SNOWFLAKE_ROLE", "")
	t.Setenv("SNOWFLAKE_WAREHOUSE", "")
	t.Setenv("SNOWFLAKE_USER", "tester")
	sc := cfg.SnowflakeConn()
	if sc.Account != "acct" || sc.Role != "reader" || sc.Warehouse != "xs_wh" || sc.User != "tester" {
		t.Fatalf("snowflake conn %+v", sc)
	}
}
