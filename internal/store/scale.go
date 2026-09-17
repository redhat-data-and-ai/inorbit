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
		AirflowPoll:         "O(deployments × listed DAGs) to enumerate, then O(matched DAGs) latest-run fetches (8 workers). Extra products on the same deployments do not re-list Airflow; run fetches grow with matched DAGs.",
		QualityPoll:         "O(data products) sequential queries on one Snowflake session. Missing ValidX/Elementary tables are skipped. At 200 products budget 1–5 minutes unless you raise the quality interval.",
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
