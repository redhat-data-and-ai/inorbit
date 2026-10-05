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
	DefaultValidationSchema    = "QUALITY"
	DefaultValidationTable     = "VALIDATION_RESULT"
	DefaultDBTSchema           = "DBTLOGS"
	DefaultDBTTable            = "ELEMENTARY_TEST_RESULTS"
	DefaultLineageDatabase     = "INORBIT_DB"
	DefaultLineageSchema       = "MARTS"
	DefaultLineageTable        = "DP_LINEAGE"
	DefaultPipelineTable       = "PIPELINE_STATUS"
	DefaultHealthSnapshotTable = "HEALTH_SCORE_SNAPSHOT"
)

// File is the on-disk live/demo JSON config. ${ENV} values are expanded.
// Data product ids, DAG maps, and quality tables belong here — not in Go.
type File struct {
	Astro          AstroConfig     `json:"astro"`
	Fivetran       FivetranConfig  `json:"fivetran"`
	Snowflake      SnowflakeConfig `json:"snowflake"`
	Lineage        LineageConfig   `json:"lineage"`
	Pipeline       LineageConfig   `json:"pipeline"`
	HealthSnapshot LineageConfig   `json:"health_snapshot"`
	DataProducts   []ProductConfig `json:"data_products"`
	Demo           DemoSnapshot    `json:"demo"`
}

// LineageConfig points at the warehouse lineage mart (not a live Airflow poll).
type LineageConfig struct {
	Enabled  *bool  `json:"enabled"`
	Database string `json:"database"`
	Schema   string `json:"schema"`
	Table    string `json:"table"`
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

// FivetranConfig is the REST API poll for source-aligned connectors.
// Key and secret stay in env; this block only names those variables.
type FivetranConfig struct {
	APIKeyEnv    string   `json:"api_key_env"`
	APISecretEnv string   `json:"api_secret_env"`
	BaseURL      string   `json:"base_url"`
	DashboardURL string   `json:"dashboard_url"`
	GroupIDs     []string `json:"group_ids"`
}

type ProductConfig struct {
	ID           string               `json:"id"`
	Name         string               `json:"name"`
	Type         string               `json:"type"`
	OwnerTeam    string               `json:"owner_team"`
	SlackChannel string               `json:"slack_channel"`
	ValidationDB string               `json:"validation_database"`
	Quality      ProductQualityConfig `json:"quality"`
	DAGIDs       []string             `json:"dag_ids"`
	FivetranIDs  []string             `json:"fivetran_connector_ids"`
}

// DemoSnapshot is in-memory sample pipeline/quality state for -mode=demo.
// Times are durations before "now" (Go ParseDuration), e.g. "95m", "2h".
type DemoSnapshot struct {
	DAGs       []DemoDAG       `json:"dags"`
	Checks     []DemoCheck     `json:"checks"`
	Lineage    []DemoLineage   `json:"lineage"`
	Connectors []DemoConnector `json:"connectors"`
}

type DemoLineage struct {
	DataProductID         string            `json:"data_product_id"`
	Status                string            `json:"status"`
	HealthScore           *float64          `json:"health_score"`
	UpstreamSources       []DemoLineageNode `json:"upstream_sources"`
	DownstreamConsumers   []DemoLineageNode `json:"downstream_consumers"`
	BlastRadiusCount      int               `json:"blast_radius_count"`
	BlastRadiusScore      string            `json:"blast_radius_score"`
	ServiceAccountCount   int               `json:"service_account_count"`
	ConsumerGroupCount    int               `json:"consumer_group_count"`
	DirectDPConsumerCount int               `json:"direct_dp_consumer_count"`
	MartSchemas           []string          `json:"mart_schemas,omitempty"`
}

type DemoLineageNode struct {
	Name             string   `json:"name"`
	Type             string   `json:"type"`
	Status           string   `json:"status"`
	HealthScore      *float64 `json:"health_score"`
	Schema           string   `json:"schema,omitempty"`
	ConnectorService string   `json:"connector_service,omitempty"`
	ConnectorType    string   `json:"connector_type,omitempty"`
	ConnectionCount  int      `json:"connection_count,omitempty"`
	PausedCount      int      `json:"paused_count,omitempty"`
	GroupName        string   `json:"group_name,omitempty"`
	DashboardURL     string   `json:"dashboard_url,omitempty"`
}

type DemoConnector struct {
	DataProductID string  `json:"data_product_id"`
	ID            string  `json:"connection_id"`
	Schema        string  `json:"schema"`
	Service       string  `json:"service"`
	GroupName     string  `json:"group_name"`
	GroupID       string  `json:"group_id"`
	Paused        bool    `json:"paused"`
	Status        string  `json:"status"`
	DashboardURL  string  `json:"dashboard_url"`
	SucceededAgo  string  `json:"succeeded_ago"`
	FailedAgo     string  `json:"failed_ago"`
	IntervalMins  float64 `json:"sync_frequency_mins"`
	TriggerType   string  `json:"trigger_type"`
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
	Validation QualityTableConfig `json:"validation"`
	DBT        QualityTableConfig `json:"dbt"`
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
	if cfg.Fivetran.APIKeyEnv == "" {
		cfg.Fivetran.APIKeyEnv = "FIVETRAN_API_KEY"
	}
	if cfg.Fivetran.APISecretEnv == "" {
		cfg.Fivetran.APISecretEnv = "FIVETRAN_API_SECRET"
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
		// Deployment root is {org}/{deployment_id} when airflow_api_url is omitted.
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
		vxTable := resolveQualityTable(p.Quality.Validation, firstNonEmpty(p.ValidationDB, defaultDPDatabase(id)), DefaultValidationSchema, DefaultValidationTable, true)
		dbtTable := resolveQualityTable(p.Quality.DBT, firstNonEmpty(p.Quality.DBT.Database, vxTable.Database), DefaultDBTSchema, DefaultDBTTable, false)
		out = append(out, domain.DataProduct{
			ID:           strings.ToLower(strings.TrimSpace(id)),
			Name:         strings.ToLower(strings.TrimSpace(name)),
			Type:         p.Type,
			OwnerTeam:    p.OwnerTeam,
			SlackChannel: p.SlackChannel,
			ValidationDB: vxTable.Database,
			Validation:   vxTable,
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

// FivetranMap is an optional explicit connector id → data_product_id overlay.
func (c File) FivetranMap() map[string]string {
	out := map[string]string{}
	for _, p := range c.DataProducts {
		id := p.ID
		if id == "" {
			id = p.Name
		}
		id = strings.ToLower(strings.TrimSpace(id))
		for _, connID := range p.FivetranIDs {
			connID = strings.TrimSpace(connID)
			if connID != "" {
				out[connID] = id
			}
		}
	}
	return out
}

func (c File) FivetranKey() string {
	if v := strings.TrimSpace(os.Getenv(c.Fivetran.APIKeyEnv)); v != "" {
		return v
	}
	for _, k := range []string{"FIVETRAN_API_KEY", "FIVETRAN_KEY"} {
		if v := strings.TrimSpace(os.Getenv(k)); v != "" {
			return v
		}
	}
	return ""
}

func (c File) FivetranSecret() string {
	if v := strings.TrimSpace(os.Getenv(c.Fivetran.APISecretEnv)); v != "" {
		return v
	}
	for _, k := range []string{"FIVETRAN_API_SECRET", "FIVETRAN_SECRET"} {
		if v := strings.TrimSpace(os.Getenv(k)); v != "" {
			return v
		}
	}
	return ""
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

// LineageTable is the warehouse mart used for the Lineage tab (not live).
func (c File) LineageTable() domain.QualityTable {
	return c.martTable(c.Lineage, DefaultLineageTable, false)
}

// PipelineTable is the warehouse mart used to fill Pipeline when a product's
// DAGs are not on a listed Airflow deployment.
func (c File) PipelineTable() domain.QualityTable {
	return c.martTable(c.Pipeline, DefaultPipelineTable, true)
}

// HealthSnapshotTable is the warehouse daily health series used for the
// overview trend (not the 15s SLA clock).
func (c File) HealthSnapshotTable() domain.QualityTable {
	return c.martTable(c.HealthSnapshot, DefaultHealthSnapshotTable, true)
}

func (c File) martTable(block LineageConfig, defaultTable string, inheritLineage bool) domain.QualityTable {
	enabled := true
	if block.Enabled != nil {
		enabled = *block.Enabled
	}
	db, schema := block.Database, block.Schema
	if inheritLineage {
		db = firstNonEmpty(db, c.Lineage.Database)
		schema = firstNonEmpty(schema, c.Lineage.Schema)
	}
	db = firstNonEmpty(db, DefaultLineageDatabase)
	schema = firstNonEmpty(schema, DefaultLineageSchema)
	table := firstNonEmpty(block.Table, defaultTable)
	if db == "" {
		enabled = false
	}
	return domain.QualityTable{Enabled: enabled, Database: db, Schema: schema, Table: table}
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
