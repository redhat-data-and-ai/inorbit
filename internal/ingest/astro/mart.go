package astro

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/inorbit/inorbit/internal/domain"
)

// MapMart turns warehouse PIPELINE_STATUS rows into DAGs for configured products.
func MapMart(rows []map[string]any, products []domain.DataProduct) []domain.DAG {
	idx := map[string]domain.DataProduct{}
	for _, p := range products {
		idx[canon(p.ID)] = p
		if canon(p.Name) != "" {
			idx[canon(p.Name)] = p
		}
	}
	var out []domain.DAG
	for _, rec := range rows {
		d, ok := dagFromMart(rec)
		if !ok {
			continue
		}
		p, ok := idx[canon(d.DataProductID)]
		if !ok {
			p, ok = idx[canon(d.DataProductName)]
		}
		if !ok {
			continue
		}
		d.DataProductID = p.ID
		d.DataProductName = p.Name
		out = append(out, d)
	}
	return out
}

func dagFromMart(rec map[string]any) (domain.DAG, bool) {
	dagID := martStr(rec, "DAG_ID", "PIPELINE_NAME")
	if dagID == "" {
		return domain.DAG{}, false
	}
	d := domain.DAG{
		DataProductID:    martStr(rec, "DATA_PRODUCT_ID"),
		DataProductName:  martStr(rec, "DATA_PRODUCT_NAME"),
		DeploymentName:   martStr(rec, "ASTRO_DEPLOYMENT_NAME"),
		DAGID:            dagID,
		RunID:            martStr(rec, "EXTERNAL_RUN_ID"),
		Status:           strings.ToUpper(martStr(rec, "DAG_STATUS", "STATUS")),
		ErrorMessage:     martStr(rec, "ERROR_MESSAGE"),
		IsPrimary:        martBool(rec, "IS_PRIMARY_DAG"),
		IsPaused:         martBool(rec, "DAG_IS_PAUSED", "IS_PAUSED"),
		IsCustom:         martBool(rec, "DAG_IS_CUSTOM", "IS_CUSTOM_DAG_TAG"),
		SilentMonitored:  martBool(rec, "IS_SILENT_FAILURE_MONITORED"),
		AstroURL:         martStr(rec, "ASTRO_URL"),
		TriggerType:      martStr(rec, "TRIGGER_TYPE"),
		PipelineType:     firstNonEmptyMart(martStr(rec, "PIPELINE_TYPE"), "DAG"),
		FrequencyDisplay: martStr(rec, "DAG_FREQUENCY_DISPLAY", "DAG_EXPECTED_FREQUENCY"),
		Runs7d:           martInt(rec, "DAG_RUNS_7D"),
		Runs30d:          martInt(rec, "DAG_RUNS_30D"),
		Runs90d:          martInt(rec, "DAG_RUNS_90D"),
	}
	d.StartedAt = martTime(rec, "DAG_STARTED_AT", "STARTED_AT")
	d.CompletedAt = martTime(rec, "DAG_COMPLETED_AT", "COMPLETED_AT")
	d.LastSuccessAt = martTime(rec, "DAG_LAST_SUCCESSFUL_RUN_AT", "LAST_SUCCESSFUL_AT")
	d.NextExpectedAt = martTime(rec, "DAG_NEXT_EXPECTED_AT", "NEXT_EXPECTED_AT")
	d.DurationSeconds = martFloat(rec, "DAG_DURATION_SECONDS", "DURATION_SECONDS")
	if v := martFloatPtr(rec, "DAG_EXPECTED_INTERVAL_MINS", "EXPECTED_INTERVAL_MINS"); v != nil {
		d.IntervalMins = v
	}
	if v := martFloatPtr(rec, "DAG_SLA_MINUTES", "SLA_MINUTES"); v != nil {
		d.SLAMinutes = v
	}
	d.Reliability7d = martFloatPtr(rec, "DAG_RELIABILITY_7D", "RELIABILITY_7D")
	d.Reliability30d = martFloatPtr(rec, "DAG_RELIABILITY_30D", "RELIABILITY_30D")
	d.Reliability90d = martFloatPtr(rec, "DAG_RELIABILITY_90D", "RELIABILITY_90D")
	d.ReliabilityStatus7d = martStr(rec, "DAG_RELIABILITY_STATUS_7D")
	d.ReliabilityStatus30d = martStr(rec, "DAG_RELIABILITY_STATUS_30D")
	d.ReliabilityStatus90d = martStr(rec, "DAG_RELIABILITY_STATUS_90D")
	if d.DataProductID == "" {
		d.DataProductID = d.DataProductName
	}
	if d.PipelineType == "" {
		d.PipelineType = "DAG"
	}
	return d, true
}

func firstNonEmptyMart(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func martFirst(rec map[string]any, keys ...string) any {
	for _, k := range keys {
		if v, ok := rec[k]; ok && v != nil {
			return v
		}
		for rk, rv := range rec {
			if strings.EqualFold(rk, k) && rv != nil {
				return rv
			}
		}
	}
	return nil
}

func martStr(rec map[string]any, keys ...string) string {
	v := martFirst(rec, keys...)
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t)
	case []byte:
		return strings.TrimSpace(string(t))
	default:
		b, err := json.Marshal(t)
		if err != nil {
			return ""
		}
		return strings.TrimSpace(strings.Trim(string(b), `"`))
	}
}

func martBool(rec map[string]any, keys ...string) bool {
	v := martFirst(rec, keys...)
	if v == nil {
		return false
	}
	switch t := v.(type) {
	case bool:
		return t
	case int:
		return t != 0
	case int64:
		return t != 0
	case float64:
		return t != 0
	case string:
		s := strings.ToLower(strings.TrimSpace(t))
		return s == "true" || s == "1" || s == "yes"
	default:
		return false
	}
}

func martInt(rec map[string]any, keys ...string) int {
	v := martFirst(rec, keys...)
	if v == nil {
		return 0
	}
	switch t := v.(type) {
	case int:
		return t
	case int32:
		return int(t)
	case int64:
		return int(t)
	case float64:
		return int(t)
	case json.Number:
		n, err := t.Int64()
		if err == nil {
			return int(n)
		}
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(t))
		if err == nil {
			return n
		}
	}
	return 0
}

func martFloat(rec map[string]any, keys ...string) float64 {
	if p := martFloatPtr(rec, keys...); p != nil {
		return *p
	}
	return 0
}

func martFloatPtr(rec map[string]any, keys ...string) *float64 {
	v := martFirst(rec, keys...)
	if v == nil {
		return nil
	}
	var f float64
	switch t := v.(type) {
	case float64:
		f = t
	case float32:
		f = float64(t)
	case int:
		f = float64(t)
	case int64:
		f = float64(t)
	case json.Number:
		n, err := t.Float64()
		if err != nil {
			return nil
		}
		f = n
	case string:
		n, err := strconv.ParseFloat(strings.TrimSpace(t), 64)
		if err != nil {
			return nil
		}
		f = n
	default:
		return nil
	}
	return &f
}

func martTime(rec map[string]any, keys ...string) *time.Time {
	v := martFirst(rec, keys...)
	if v == nil {
		return nil
	}
	switch t := v.(type) {
	case time.Time:
		u := t.UTC()
		return &u
	case string:
		s := strings.TrimSpace(t)
		if s == "" {
			return nil
		}
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05.000", "2006-01-02 15:04:05"} {
			if parsed, err := time.Parse(layout, s); err == nil {
				u := parsed.UTC()
				return &u
			}
		}
	}
	return nil
}
