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
    {"id": "beta", "name": "beta", "fivetran_connector_ids": ["conn-orders"]}
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
	if cfg.FivetranMap()["conn-orders"] != "beta" {
		t.Fatalf("fivetran map %+v", cfg.FivetranMap())
	}
	var alphaDB string
	for _, p := range cfg.Products() {
		if p.ID == "alpha" {
			alphaDB = p.ValidationDB
		}
	}
	if alphaDB != "ALPHA_DB" {
		t.Fatalf("validation db %q", alphaDB)
	}
	for _, p := range cfg.Products() {
		if p.ID == "alpha" {
			if !p.Validation.Enabled || p.Validation.Schema != "QUALITY" || p.Validation.Table != "VALIDATION_RESULT" {
				t.Fatalf("alpha validation %+v", p.Validation)
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

func TestDeploymentsSkipEmptyIDs(t *testing.T) {
	t.Setenv("ASTRO_TOKEN", "tok")
	t.Setenv("ASTRO_ORG_URL", "https://org.example.com")
	t.Setenv("INORBIT_ASTRO_DEPLOYMENT_ID", "dep123")
	t.Setenv("INORBIT_ASTRO_EXTRA_PROD_DEPLOYMENT_ID", "")
	path := filepath.Join(t.TempDir(), "live.json")
	body := `{
  "astro": {
    "org_url": "${ASTRO_ORG_URL}",
    "deployments": [
      {"name": "prod", "id": "${INORBIT_ASTRO_DEPLOYMENT_ID}"},
      {"name": "extra-prod", "id": "${INORBIT_ASTRO_EXTRA_PROD_DEPLOYMENT_ID}"}
    ]
  },
  "data_products": [{"id": "alpha"}]
}`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	deps := cfg.Deployments()
	if len(deps) != 1 || deps[0].Name != "prod" {
		t.Fatalf("empty extra id must be skipped, got %+v", deps)
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
        "validation": {"enabled": true, "database": "ALPHA_DB", "schema": "QUALITY", "table": "VALIDATION_RESULT"},
        "dbt": {"enabled": false, "database": "ALPHA_DB", "schema": "DBTLOGS", "table": "ELEMENTARY_TEST_RESULTS"}
      }
    },
    {
      "id": "beta",
      "quality": {
        "validation": {"enabled": true, "database": "BETA_DB"},
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
	if !fc.Validation.Enabled || fc.Validation.Database != "ALPHA_DB" || fc.Validation.Schema != "QUALITY" || fc.Validation.Table != "VALIDATION_RESULT" {
		t.Fatalf("alpha validation %+v", fc.Validation)
	}
	if fc.DBTLogs.Enabled {
		t.Fatalf("alpha dbt should be off: %+v", fc.DBTLogs)
	}
	bm := got["beta"]
	if !bm.Validation.Enabled || bm.Validation.Database != "BETA_DB" || bm.Validation.Schema != "QUALITY" {
		t.Fatalf("beta validation %+v", bm.Validation)
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

func TestFivetranCredentialsFromEnv(t *testing.T) {
	t.Setenv("FIVETRAN_API_KEY", "k1")
	t.Setenv("FIVETRAN_API_SECRET", "s1")
	path := filepath.Join(t.TempDir(), "live.json")
	if err := os.WriteFile(path, []byte(`{"data_products":[{"id":"alpha","fivetran_connector_ids":["c1"]}]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.FivetranKey() != "k1" || cfg.FivetranSecret() != "s1" {
		t.Fatalf("key %q secret %q", cfg.FivetranKey(), cfg.FivetranSecret())
	}
	if cfg.FivetranMap()["c1"] != "alpha" {
		t.Fatalf("map %+v", cfg.FivetranMap())
	}
}

func TestLineageTableDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "live.json")
	if err := os.WriteFile(path, []byte(`{"data_products":[{"id":"alpha"}]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	lin := cfg.LineageTable()
	if !lin.Enabled || lin.Database != "INORBIT_DB" || lin.Schema != "MARTS" || lin.Table != "DP_LINEAGE" {
		t.Fatalf("lineage %+v", lin)
	}
	pipe := cfg.PipelineTable()
	if !pipe.Enabled || pipe.Database != "INORBIT_DB" || pipe.Schema != "MARTS" || pipe.Table != "PIPELINE_STATUS" {
		t.Fatalf("pipeline %+v", pipe)
	}
	hs := cfg.HealthSnapshotTable()
	if !hs.Enabled || hs.Database != "INORBIT_DB" || hs.Schema != "MARTS" || hs.Table != "HEALTH_SCORE_SNAPSHOT" {
		t.Fatalf("health snapshot %+v", hs)
	}
}
