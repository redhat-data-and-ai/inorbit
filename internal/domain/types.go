package domain

import "time"

// JSON names match warehouse mart columns so a UI can swap transports later.

type HealthLabel string

const (
	HealthTrusted HealthLabel = "TRUSTED"
	HealthCaution HealthLabel = "CAUTION"
	HealthAtRisk  HealthLabel = "AT_RISK"
)

type FreshnessBand string

const (
	FreshnessGreen  FreshnessBand = "GREEN"
	FreshnessYellow FreshnessBand = "YELLOW"
	FreshnessRed    FreshnessBand = "RED"
)

type PipelineSLA string

const (
	SLAOK     PipelineSLA = "OK"
	SLAAtRisk PipelineSLA = "AT_RISK"
	SLABreach PipelineSLA = "BREACH"
)

type OverallStatus string

const (
	OverallFailed  OverallStatus = "FAILED"
	OverallDelayed OverallStatus = "DELAYED"
	OverallPaused  OverallStatus = "PAUSED"
	OverallRunning OverallStatus = "RUNNING"
	OverallAtRisk  OverallStatus = "AT_RISK"
	OverallCaution OverallStatus = "CAUTION"
	OverallTrusted OverallStatus = "TRUSTED"
)

type CheckStatus string

const (
	CheckPassed  CheckStatus = "PASSED"
	CheckFailed  CheckStatus = "FAILED"
	CheckWarning CheckStatus = "WARNING"
)

type Severity string

const (
	SevCritical Severity = "CRITICAL"
	SevHigh     Severity = "HIGH"
	SevMedium   Severity = "MEDIUM"
	SevLow      Severity = "LOW"
)

type Dimension string

const (
	DimFreshness    Dimension = "FRESHNESS"
	DimAccuracy     Dimension = "ACCURACY"
	DimConsistency  Dimension = "CONSISTENCY"
	DimCompleteness Dimension = "COMPLETENESS"
	DimValidity     Dimension = "VALIDITY"
	DimUniqueness   Dimension = "UNIQUENESS"
	DimUnknown      Dimension = "UNKNOWN"
)

type SourceType string

const (
	SrcValidation     SourceType = "VALIDATION"
	SrcDBTTest        SourceType = "DBT_TEST"
	SrcAstroFreshness SourceType = "ASTRO_FRESHNESS"
	SrcAstroPipeline  SourceType = "ASTRO_PIPELINE"
)

type Audience string

const (
	AudienceDeveloper Audience = "developer"
	AudienceBusiness  Audience = "business"
)

// QualityTable is one warehouse relation InOrbit reads for live quality.
// Validation: {database}.{schema}.{table} (defaults QUALITY.VALIDATION_RESULT).
// dbt: {database}.{schema}.{table} (defaults DBTLOGS.ELEMENTARY_TEST_RESULTS).
type QualityTable struct {
	Enabled  bool   `json:"enabled"`
	Database string `json:"database,omitempty"`
	Schema   string `json:"schema,omitempty"`
	Table    string `json:"table,omitempty"`
}

type DataProduct struct {
	ID           string       `json:"data_product_id"`
	Name         string       `json:"data_product_name"`
	OwnerTeam    string       `json:"owner_team,omitempty"`
	SlackChannel string       `json:"slack_channel,omitempty"`
	Type         string       `json:"dp_type,omitempty"`
	ValidationDB string       `json:"validation_database,omitempty"`
	Validation   QualityTable `json:"validation,omitempty"`
	DBTLogs      QualityTable `json:"dbt_logs,omitempty"`
}

type Check struct {
	ID                    string      `json:"check_id"`
	DataProductID         string      `json:"data_product_id"`
	DataProductName       string      `json:"data_product_name"`
	Name                  string      `json:"check_name"`
	Description           string      `json:"check_description,omitempty"`
	Dimension             Dimension   `json:"dimension"`
	Severity              Severity    `json:"severity"`
	Status                CheckStatus `json:"status"`
	SourceType            SourceType  `json:"source_type"`
	SourceDisplayName     string      `json:"source_display_name,omitempty"`
	SourceTable           string      `json:"source_table,omitempty"`
	IsCDE                 bool        `json:"is_cde"`
	IsFlapping            bool        `json:"is_flapping"`
	BlastRadiusMultiplier float64     `json:"blast_radius_multiplier"`
	BasePoints            float64     `json:"base_points"`
	CDEMultiplier         float64     `json:"cde_multiplier"`
	TimeDecayMultiplier   float64     `json:"time_decay_multiplier"`
	FinalDeduction        float64     `json:"final_deduction"`
	Element               string      `json:"element,omitempty"`
	ExecutedAt            time.Time   `json:"executed_at"`
	FirstFailedAt         *time.Time  `json:"first_failed_at,omitempty"`
}

// DAG is the latest known Astro state for one DAG (ingest output, not history).
type DAG struct {
	DataProductID   string     `json:"data_product_id"`
	DataProductName string     `json:"data_product_name"`
	DeploymentName  string     `json:"astro_deployment_name"`
	DeploymentID    string     `json:"astro_deployment_id,omitempty"`
	DAGID           string     `json:"dag_id"`
	RunID           string     `json:"external_run_id,omitempty"`
	Status          string     `json:"dag_status"`
	StartedAt       *time.Time `json:"dag_started_at,omitempty"`
	CompletedAt     *time.Time `json:"dag_completed_at,omitempty"`
	DurationSeconds float64    `json:"dag_duration_seconds"`
	ErrorMessage    string     `json:"error_message,omitempty"`
	IsPrimary       bool       `json:"is_primary_dag"`
	IsPaused        bool       `json:"dag_is_paused"`
	IsCustom        bool       `json:"dag_is_custom"`
	SilentMonitored bool       `json:"is_silent_failure_monitored"`
	IntervalMins    *float64   `json:"dag_expected_interval_mins,omitempty"`
	SLAMinutes      *float64   `json:"dag_sla_minutes,omitempty"`
	NextExpectedAt  *time.Time `json:"dag_next_expected_at,omitempty"`
	// AstroURL is the DAG overview, same shape as marts.pipeline_status.astro_url:
	// {org}/{deployment_id}/dags/{dag_id}. Not /runs/{run_id} (Astro DAG RBAC 403s that).
	AstroURL      string     `json:"astro_url,omitempty"`
	TriggerType   string     `json:"trigger_type,omitempty"`
	LastSuccessAt *time.Time `json:"last_successful_at,omitempty"`
	// PipelineType is always DAG for Astro (marts.pipeline_status.pipeline_type).
	PipelineType         string   `json:"pipeline_type,omitempty"`
	FrequencyDisplay     string   `json:"dag_frequency_display,omitempty"`
	Runs7d               int      `json:"dag_runs_7d"`
	Reliability7d        *float64 `json:"dag_reliability_7d,omitempty"`
	ReliabilityStatus7d  string   `json:"dag_reliability_status_7d,omitempty"`
	Runs30d              int      `json:"dag_runs_30d"`
	Reliability30d       *float64 `json:"dag_reliability_30d,omitempty"`
	ReliabilityStatus30d string   `json:"dag_reliability_status_30d,omitempty"`
	Runs90d              int      `json:"dag_runs_90d"`
	Reliability90d       *float64 `json:"dag_reliability_90d,omitempty"`
	ReliabilityStatus90d string   `json:"dag_reliability_status_90d,omitempty"`
}

type HealthStatus struct {
	DataProductID          string      `json:"data_product_id"`
	DataProductName        string      `json:"data_product_name"`
	Status                 HealthLabel `json:"status"`
	HealthScore            float64     `json:"health_score"`
	FreshnessScore         *float64    `json:"freshness_score"`
	AccuracyScore          *float64    `json:"accuracy_score"`
	ConsistencyScore       *float64    `json:"consistency_score"`
	CompletenessScore      *float64    `json:"completeness_score"`
	ValidityScore          *float64    `json:"validity_score"`
	UniquenessScore        *float64    `json:"uniqueness_score"`
	MeasuredDimensionCount int         `json:"measured_dimension_count"`
	CoveragePct            float64     `json:"coverage_pct"`
	CoverageCapApplied     bool        `json:"coverage_cap_applied"`
	TotalChecks            int         `json:"total_checks"`
	FailedChecks           int         `json:"failed_checks"`
	WarningChecks          int         `json:"warning_checks"`
	AstroCheckCount        int         `json:"astro_check_count"`
	ValidationCheckCount   int         `json:"validation_check_count"`
	StatusMessage          string      `json:"status_message"`
	EvaluatedAt            time.Time   `json:"evaluated_at"`
}

// HealthPoint is one sample on the live health trend line.
type HealthPoint struct {
	At     time.Time   `json:"t"`
	Score  float64     `json:"health_score"`
	Status HealthLabel `json:"status"`
}

type FreshnessSLA struct {
	DataProductID        string        `json:"data_product_id"`
	DataProductName      string        `json:"data_product_name"`
	ExpectedFrequency    string        `json:"expected_frequency,omitempty"`
	ExpectedIntervalMins *float64      `json:"expected_interval_mins,omitempty"`
	SLAMinutes           *float64      `json:"sla_minutes,omitempty"`
	LastSuccessfulAt     *time.Time    `json:"last_successful_at,omitempty"`
	CurrentDelayMins     *float64      `json:"current_delay_mins,omitempty"`
	FreshnessStatus      FreshnessBand `json:"freshness_status"`
	PipelineSLABreach    PipelineSLA   `json:"pipeline_sla_breach"`
	StatusReason         string        `json:"status_reason,omitempty"`
	CheckedAt            time.Time     `json:"checked_at"`
}

type PipelineStatus struct {
	DAG
	DAGFreshnessStatus          HealthLabel   `json:"dag_freshness_status"`
	DAGPipelineSLAStatus        PipelineSLA   `json:"dag_pipeline_sla_status"`
	DAGOverallStatus            OverallStatus `json:"dag_overall_status"`
	DAGOverallStatusDescription string        `json:"dag_overall_status_description,omitempty"`
	DAGDataAgeMins              *float64      `json:"dag_data_age_mins,omitempty"`
	ComputedAt                  time.Time     `json:"computed_at"`
}

type Subscription struct {
	ID            string    `json:"id"`
	DataProductID string    `json:"data_product_id"`
	Audience      Audience  `json:"audience"`
	SlackChannel  string    `json:"slack_channel"`
	CreatedBy     string    `json:"created_by,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
}

type Snapshot struct {
	DataProduct DataProduct      `json:"data_product"`
	Health      HealthStatus     `json:"health"`
	Freshness   FreshnessSLA     `json:"freshness"`
	Pipeline    []PipelineStatus `json:"pipeline"`
	Quality     []Check          `json:"quality"`
	UpdatedAt   time.Time        `json:"updated_at"`
}

type PollMeta struct {
	Mode                  string        `json:"mode"`
	ClockIntervalSeconds  int           `json:"clock_interval_seconds"`
	AstroRunPollSeconds   int           `json:"astro_run_poll_seconds"`
	AstroTagPollSeconds   int           `json:"astro_tag_poll_seconds"`
	QualityPollSeconds    int           `json:"quality_poll_seconds"`
	LastAstroRunPoll      time.Time     `json:"last_astro_run_poll,omitempty"`
	LastAstroTagPoll      time.Time     `json:"last_astro_tag_poll,omitempty"`
	LastQualityPoll       time.Time     `json:"last_quality_poll,omitempty"`
	LastClockTick         time.Time     `json:"last_clock_tick,omitempty"`
	DataProductCount      int           `json:"data_product_count"`
	SnapshotBytesEstimate int           `json:"snapshot_bytes_estimate"`
	Scale                 ScaleEstimate `json:"scale"`
	Warnings              []string      `json:"warnings,omitempty"`
}

// ScaleTargetDataProducts is the fleet size the process is sized for.
const ScaleTargetDataProducts = 200

// ScaleEstimate projects in-memory snapshot size and poll cost to ScaleTargetDataProducts.
type ScaleEstimate struct {
	TargetDataProducts                 int    `json:"target_data_products"`
	CurrentDataProducts                int    `json:"current_data_products"`
	DAGCount                           int    `json:"dag_count"`
	CheckCount                         int    `json:"check_count"`
	HealthHistoryPoints                int    `json:"health_history_points"`
	SnapshotJSONBytes                  int    `json:"snapshot_json_bytes"`
	BytesPerProduct                    int    `json:"bytes_per_product"`
	EstimatedSnapshotJSONBytesAtTarget int    `json:"estimated_snapshot_json_bytes_at_target"`
	EstimatedRSSBytesAtTarget          int    `json:"estimated_rss_bytes_at_target"`
	AirflowPoll                        string `json:"airflow_poll"`
	QualityPoll                        string `json:"quality_poll"`
	ClockTick                          string `json:"clock_tick"`
}
