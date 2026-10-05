package healthsnap

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/inorbit/inorbit/internal/domain"
)

// Map turns warehouse daily health rows into per-product series (oldest first).
func Map(rows []map[string]any, products []domain.DataProduct) map[string][]domain.HealthPoint {
	byKey := map[string][]domain.HealthPoint{}
	for _, rec := range rows {
		pt, id, name, ok := fromRow(rec)
		if !ok {
			continue
		}
		for _, k := range []string{canon(id), canon(name)} {
			if k != "" {
				byKey[k] = append(byKey[k], pt)
			}
		}
	}
	out := map[string][]domain.HealthPoint{}
	for _, p := range products {
		pts := byKey[canon(p.ID)]
		if len(pts) == 0 {
			pts = byKey[canon(p.Name)]
		}
		if len(pts) == 0 {
			continue
		}
		out[p.ID] = dedupeDays(pts)
	}
	return out
}

func fromRow(rec map[string]any) (domain.HealthPoint, string, string, bool) {
	id := str(rec, "DATA_PRODUCT_ID", "data_product_id")
	name := str(rec, "DATA_PRODUCT_NAME", "data_product_name")
	if id == "" && name == "" {
		return domain.HealthPoint{}, "", "", false
	}
	at := asTime(first(rec, "SNAPSHOT_DATE", "snapshot_date", "SNAPSHORTED_AT", "snapshotted_at", "COMPUTED_AT", "computed_at"))
	if at.IsZero() {
		return domain.HealthPoint{}, "", "", false
	}
	sc := floatPtr(first(rec, "HEALTH_SCORE", "health_score", "CONSOLIDATED_SCORE", "consolidated_score"))
	if sc == nil {
		return domain.HealthPoint{}, "", "", false
	}
	st := parseStatus(str(rec, "STATUS", "status"), *sc)
	pt := domain.HealthPoint{
		At:                     at,
		Score:                  *sc,
		Status:                 st,
		TotalChecks:            asInt(first(rec, "TOTAL_CHECKS", "total_checks"), 0),
		FailedChecks:           asInt(first(rec, "FAILED_CHECKS", "failed_checks"), 0),
		FreshnessScore:         floatPtr(first(rec, "FRESHNESS_SCORE", "freshness_score")),
		AccuracyScore:          floatPtr(first(rec, "ACCURACY_SCORE", "accuracy_score")),
		ConsistencyScore:       floatPtr(first(rec, "CONSISTENCY_SCORE", "consistency_score")),
		CompletenessScore:      floatPtr(first(rec, "COMPLETENESS_SCORE", "completeness_score")),
		ValidityScore:          floatPtr(first(rec, "VALIDITY_SCORE", "validity_score")),
		UniquenessScore:        floatPtr(first(rec, "UNIQUENESS_SCORE", "uniqueness_score")),
		MeasuredDimensionCount: asInt(first(rec, "MEASURED_DIMENSION_COUNT", "measured_dimension_count"), 0),
	}
	return pt, id, name, true
}

func parseStatus(raw string, score float64) domain.HealthLabel {
	switch strings.ToUpper(strings.TrimSpace(strings.ReplaceAll(raw, " ", "_"))) {
	case "TRUSTED":
		return domain.HealthTrusted
	case "CAUTION":
		return domain.HealthCaution
	case "AT_RISK", "ATRISK":
		return domain.HealthAtRisk
	}
	if score >= 80 {
		return domain.HealthTrusted
	}
	if score >= 60 {
		return domain.HealthCaution
	}
	return domain.HealthAtRisk
}

func dedupeDays(pts []domain.HealthPoint) []domain.HealthPoint {
	order := make([]time.Time, 0, len(pts))
	latest := map[time.Time]domain.HealthPoint{}
	for _, p := range pts {
		d := dayUTC(p.At)
		if _, ok := latest[d]; !ok {
			order = append(order, d)
		}
		if cur, ok := latest[d]; !ok || p.At.After(cur.At) {
			p.At = d
			latest[d] = p
		}
	}
	out := make([]domain.HealthPoint, 0, len(order))
	for _, d := range order {
		out = append(out, latest[d])
	}
	sort.Slice(out, func(i, j int) bool { return out[i].At.Before(out[j].At) })
	return out
}

func dayUTC(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

func first(rec map[string]any, keys ...string) any {
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

func str(rec map[string]any, keys ...string) string {
	v := first(rec, keys...)
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t)
	default:
		b, err := json.Marshal(t)
		if err != nil {
			return ""
		}
		return strings.TrimSpace(strings.Trim(string(b), `"`))
	}
}

func asInt(v any, fallback int) int {
	if v == nil {
		return fallback
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
	return fallback
}

func floatPtr(v any) *float64 {
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

func asTime(v any) time.Time {
	if v == nil {
		return time.Time{}
	}
	switch t := v.(type) {
	case time.Time:
		return t.UTC()
	case string:
		s := strings.TrimSpace(t)
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02", "2006-01-02 15:04:05.000", "2006-01-02 15:04:05"} {
			if parsed, err := time.Parse(layout, s); err == nil {
				return parsed.UTC()
			}
		}
	}
	return time.Time{}
}

func canon(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.ReplaceAll(s, "-", "")
	s = strings.ReplaceAll(s, "_", "")
	return s
}
