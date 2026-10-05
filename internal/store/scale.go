package store

import "github.com/inorbit/inorbit/internal/domain"

// processBaselineBytes is a conservative RSS floor for the Go runtime, HTTP
// server, Airflow client, and one Snowflake session — not the snapshot maps.
const processBaselineBytes = 48 << 20

type scaleInput struct {
	products     int
	dags         int
	checks       int
	histPoints   int
	snapshotJSON int
}

func estimateScale(in scaleInput) domain.ScaleEstimate {
	out := domain.ScaleEstimate{
		TargetDataProducts:  domain.ScaleTargetDataProducts,
		CurrentDataProducts: in.products,
		DAGCount:            in.dags,
		CheckCount:          in.checks,
		HealthHistoryPoints: in.histPoints,
		SnapshotJSONBytes:   in.snapshotJSON,
		AirflowPoll:         "O(deployments × listed DAGs) to enumerate (4 deployments at a time), then O(matched DAGs) latest-run history (8 workers, up to 400 runs/DAG). Extra products on the same deployments do not re-list Airflow. Raise the 90s interval only if a poll overruns. Fivetran is O(groups × connections) on the same interval for source-aligned products.",
		QualityPoll:         "O(data products) warehouse queries, 8 at a time after SSO, plus lineage-mart and pipeline-mart queries. Latest validation/Elementary run only — dbt tests follow the pipeline, not this poll. Missing tables are cached. Default interval 5m so the poll stays behind the warehouse watermark.",
		ClockTick:           "O(data products × DAGs) in process, no I/O. 15s ticks stay cheap at 200 products.",
	}
	if in.products > 0 {
		out.BytesPerProduct = in.snapshotJSON / in.products
	}
	per := out.BytesPerProduct
	if per <= 0 {
		per = 80 << 10 // ~80KiB/product when the process has no live snapshot yet
		out.BytesPerProduct = per
	}
	out.EstimatedSnapshotJSONBytesAtTarget = per * domain.ScaleTargetDataProducts
	histAtTarget := in.histPoints
	if in.products > 0 {
		histAtTarget = (in.histPoints * domain.ScaleTargetDataProducts) / in.products
	} else {
		histAtTarget = 180 * domain.ScaleTargetDataProducts
	}
	// JSON payload ×3 covers live maps + copies on read + encode for /v1/snapshots.
	out.EstimatedRSSBytesAtTarget = processBaselineBytes + out.EstimatedSnapshotJSONBytesAtTarget*3 + histAtTarget*64
	return out
}
