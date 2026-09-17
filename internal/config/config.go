package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/inorbit/inorbit/internal/domain"
	"github.com/inorbit/inorbit/internal/ingest/astro"
)

const (
	DefaultValidXSchema = "DATATRUST_MARTS"
	DefaultValidXTable  = "VALIDATION_RESULT"
	DefaultDBTSchema    = "DBTLOGS"
	DefaultDBTTable     = "ELEMENTARY_TEST_RESULTS"
)

// File is the on-disk live/demo JSON config. ${ENV} values are expanded.
// Data product ids, DAG maps, and quality tables belong here — not in Go.
type File struct {
	Astro        AstroConfig     `json:"astro"`
	Snowflake    SnowflakeConfig `json:"snowflake"`
	DataProducts []ProductConfig `json:"data_products"`
	Demo         DemoSnapshot    `json:"demo"`
}

// SnowflakeConfig is the shared warehouse connection. Account/role/warehouse
// belong in configs/live.json (gitignored). User/password/key stay in env.
type SnowflakeConfig struct {
	Account       string `json:"account"`
	User          string `json:"user"`
	UserEnv       string `json:"user_env"`
	Role          string `json:"role"`
	Warehouse     string `json:"warehouse"`
	Database      string `json:"database"`
	Authenticator string `json:"authenticator"`
}

type AstroConfig struct {
	TokenEnv    string             `json:"token_env"`
	OrgURL      string             `json:"org_url"`
	Deployments []DeploymentConfig `json:"deployments"`
}

type DeploymentConfig struct {
	Name          string `json:"name"`
	ID            string `json:"id"`
	AirflowAPIURL string `json:"airflow_api_url"`
}

type ProductConfig struct {
	ID           string               `json:"id"`
	Name         string               `json:"name"`
	Type         string               `json:"type"`
	OwnerTeam    string               `json:"owner_team"`
	SlackChannel string               `json:"slack_channel"`
	ValidXDB     string               `json:"validx_database"`
	Quality      ProductQualityConfig `json:"quality"`
	DAGIDs       []string             `json:"dag_ids"`
}

// DemoSnapshot is in-memory sample pipeline/quality state for -mode=demo.
// Times are durations before "now" (Go ParseDuration), e.g. "95m", "2h".
type DemoSnapshot struct {
	DAGs   []DemoDAG   `json:"dags"`
	Checks []DemoCheck `json:"checks"`
}

type DemoDAG struct {
	DataProductID   string   `json:"data_product_id"`
	DAGID           string   `json:"dag_id"`
	DeploymentName  string   `json:"astro_deployment_name"`
	DeploymentID    string   `json:"astro_deployment_id"`
	Status          string   `json:"dag_status"`
	RunID           string   `json:"external_run_id"`
	CompletedAgo    string   `json:"completed_ago"`
	DurationSeconds float64  `json:"dag_duration_seconds"`
	IsPrimary       bool     `json:"is_primary_dag"`
	IsPaused        bool     `json:"dag_is_paused"`
	IsCustom        bool     `json:"dag_is_custom"`
	SilentMonitored bool     `json:"is_silent_failure_monitored"`
	IntervalMins    float64  `json:"dag_expected_interval_mins"`
	SLAMinutes      float64  `json:"dag_sla_minutes"`
	TriggerType     string   `json:"trigger_type"`
	PipelineType    string   `json:"pipeline_type"`
	AstroURL        string   `json:"astro_url"`
	Runs7d          int      `json:"dag_runs_7d"`
	Reliability7d   *float64 `json:"dag_reliability_7d"`
	Runs30d         int      `json:"dag_runs_30d"`
	Reliability30d  *float64 `json:"dag_reliability_30d"`
	Runs90d         int      `json:"dag_runs_90d"`
	Reliability90d  *float64 `json:"dag_reliability_90d"`
}

type DemoCheck struct {
	ID             string `json:"check_id"`
	DataProductID  string `json:"data_product_id"`
	Name           string `json:"check_name"`
	Dimension      string `json:"dimension"`
	Severity       string `json:"severity"`
	Status         string `json:"status"`
	SourceType     string `json:"source_type"`
	SourceTable    string `json:"source_table"`
	IsCDE          bool   `json:"is_cde"`
	Element        string `json:"element"`
	ExecutedAgo    string `json:"executed_ago"`
	FirstFailedAgo string `json:"first_failed_ago"`
}

type ProductQualityConfig struct {
	ValidX QualityTableConfig `json:"validx"`
	DBT    QualityTableConfig `json:"dbt"`
}

type QualityTableConfig struct {
	Enabled  *bool  `json:"enabled"`
	Database string `json:"database"`
	Schema   string `json:"schema"`
	Table    string `json:"table"`
}

func Load(path string) (File, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return File{}, err
	}
	expanded := os.Expand(string(raw), os.Getenv)
	var cfg File
	if err := json.Unmarshal([]byte(expanded), &cfg); err != nil {
		return File{}, fmt.Errorf("%s: %w", path, err)
	}
	if cfg.Astro.TokenEnv == "" {
		cfg.Astro.TokenEnv = "ASTRO_TOKEN"
	}
	if cfg.Snowflake.UserEnv == "" {
		cfg.Snowflake.UserEnv = "SNOWFLAKE_USER"
	}
	return cfg, nil
}

func (c File) Token() string {
	if v := strings.TrimSpace(os.Getenv(c.Astro.TokenEnv)); v != "" {
		return v
	}
	for _, k := range []string{"ASTRO_TOKEN", "ASTRO_API_TOKEN"} {
		if v := strings.TrimSpace(os.Getenv(k)); v != "" {
			return v
		}
	}
	return ""
}

func (c File) OrgURL() string {
	if v := strings.TrimSpace(c.Astro.OrgURL); v != "" {
		return strings.TrimRight(v, "/")
	}
	for _, k := range []string{"ASTRO_ORG_URL", "INORBIT_ASTRO_ORG_URL"} {
		if v := strings.TrimSpace(os.Getenv(k)); v != "" {
			return strings.TrimRight(v, "/")
		}
	}
	return ""
}

func (c File) Deployments() []astro.Deployment {
	org := c.OrgURL()
	out := make([]astro.Deployment, 0, len(c.Astro.Deployments))
	for _, d := range c.Astro.Deployments {
		id := strings.TrimSpace(d.ID)
		apiURL := strings.TrimSpace(d.AirflowAPIURL)
		// Match count_astro_dags.py: base = f"{org}/{deployment_id}"
		if apiURL == "" && org != "" && id != "" {
			apiURL = org + "/" + id
		}
		if apiURL == "" {
			continue
		}
		out = append(out, astro.Deployment{
			Name:          d.Name,
			ID:            id,
			AirflowAPIURL: strings.TrimRight(apiURL, "/"),
			OrgURL:        org,
		})
	}
	return out
}

func (c File) Products() []domain.DataProduct {
	out := make([]domain.DataProduct, 0, len(c.DataProducts))
	for _, p := range c.DataProducts {
		id := p.ID
		if id == "" {
			id = p.Name
		}
		name := p.Name
		if name == "" {
			name = id
		}
		vxTable := resolveQualityTable(p.Quality.ValidX, firstNonEmpty(p.ValidXDB, defaultDPDatabase(id)), DefaultValidXSchema, DefaultValidXTable, true)
		dbtTable := resolveQualityTable(p.Quality.DBT, firstNonEmpty(p.Quality.DBT.Database, vxTable.Database), DefaultDBTSchema, DefaultDBTTable, false)
		out = append(out, domain.DataProduct{
			ID:           strings.ToLower(strings.TrimSpace(id)),
			Name:         strings.ToLower(strings.TrimSpace(name)),
			Type:         p.Type,
			OwnerTeam:    p.OwnerTeam,
			SlackChannel: p.SlackChannel,
			ValidXDB:     vxTable.Database,
			ValidX:       vxTable,
			DBTLogs:      dbtTable,
		})
	}
	return out
}

// DAGMap is an optional explicit dag_id → data_product_id overlay.
func (c File) DAGMap() map[string]string {
	out := map[string]string{}
	for _, p := range c.DataProducts {
		id := p.ID
		if id == "" {
			id = p.Name
		}
		id = strings.ToLower(strings.TrimSpace(id))
		for _, dagID := range p.DAGIDs {
			dagID = strings.TrimSpace(dagID)
			if dagID != "" {
				out[dagID] = id
			}
		}
	}
	return out
}

func defaultDPDatabase(id string) string {
	return strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(id), "-", "_")) + "_DB"
}

func resolveQualityTable(cfg QualityTableConfig, defaultDB, defaultSchema, defaultTable string, defaultEnabled bool) domain.QualityTable {
	enabled := defaultEnabled
	if cfg.Enabled != nil {
		enabled = *cfg.Enabled
	}
	db := strings.TrimSpace(cfg.Database)
	if db == "" {
		db = strings.TrimSpace(defaultDB)
	}
	schema := strings.TrimSpace(cfg.Schema)
	if schema == "" {
		schema = defaultSchema
	}
	table := strings.TrimSpace(cfg.Table)
	if table == "" {
		table = defaultTable
	}
	if db == "" {
		enabled = false
	}
	return domain.QualityTable{
		Enabled:  enabled,
		Database: db,
		Schema:   schema,
		Table:    table,
	}
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

// SnowflakeConn merges live.json with env. Config wins when set so the common
// account can live in configs/live.json; env fills blanks. User comes from env.
func (c File) SnowflakeConn() SnowflakeConfig {
	s := c.Snowflake
	userEnv := s.UserEnv
	if userEnv == "" {
		userEnv = "SNOWFLAKE_USER"
	}
	return SnowflakeConfig{
		Account:       firstNonEmpty(s.Account, os.Getenv("SNOWFLAKE_ACCOUNT")),
		User:          firstNonEmpty(os.Getenv(userEnv), os.Getenv("SNOWFLAKE_USER"), s.User),
		UserEnv:       userEnv,
		Role:          firstNonEmpty(s.Role, os.Getenv("SNOWFLAKE_ROLE")),
		Warehouse:     firstNonEmpty(s.Warehouse, os.Getenv("SNOWFLAKE_WAREHOUSE")),
		Database:      firstNonEmpty(s.Database, os.Getenv("SNOWFLAKE_DATABASE")),
		Authenticator: firstNonEmpty(s.Authenticator, os.Getenv("SNOWFLAKE_AUTHENTICATOR")),
	}
}
