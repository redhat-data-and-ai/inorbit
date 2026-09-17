package quality

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/inorbit/inorbit/internal/domain"
)

func MapStatus(raw string) domain.CheckStatus {
	s := strings.ToUpper(strings.TrimSpace(raw))
	switch s {
	case "PASS", "PASSED", "SUCCESS", "OK":
		return domain.CheckPassed
	case "WARN", "WARNING":
		return domain.CheckWarning
	case "SKIP", "SKIPPED":
		return ""
	default:
		if s == "" {
			return domain.CheckFailed
		}
		return domain.CheckFailed
	}
}

func MapSeverity(raw, source string) domain.Severity {
	s := strings.ToLower(strings.TrimSpace(raw))
	switch s {
	case "critical", "error":
		if source == "dbt" && s == "error" {
			return domain.SevCritical
		}
		if s == "critical" {
			return domain.SevCritical
		}
		return domain.SevCritical
	case "high":
		return domain.SevHigh
	case "medium", "warn", "warning":
		return domain.SevMedium
	case "low":
		return domain.SevLow
	}
	if source == "dbt" {
		return domain.SevMedium
	}
	return domain.SevMedium
}

func MapDimension(raw, tags, checkName string) domain.Dimension {
	d := strings.ToUpper(strings.TrimSpace(raw))
	switch d {
	case "FRESHNESS", "ACCURACY", "CONSISTENCY", "COMPLETENESS", "VALIDITY", "UNIQUENESS":
		return domain.Dimension(d)
	case "TIMELINESS":
		return domain.DimFreshness
	case "INTEGRITY":
		return domain.DimConsistency
	}
	if from := dimensionFromTags(tags); from != "" {
		return from
	}
	if from := dimensionFromName(checkName); from != "" {
		return from
	}
	return domain.DimUnknown
}

func dimensionFromTags(tags string) domain.Dimension {
	t := strings.ToLower(tags)
	switch {
	case strings.Contains(t, "completeness"):
		return domain.DimCompleteness
	case strings.Contains(t, "accuracy"):
		return domain.DimAccuracy
	case strings.Contains(t, "validity"):
		return domain.DimValidity
	case strings.Contains(t, "consistency"):
		return domain.DimConsistency
	case strings.Contains(t, "timeliness"), strings.Contains(t, "freshness"):
		return domain.DimFreshness
	case strings.Contains(t, "uniqueness"):
		return domain.DimUniqueness
	case strings.Contains(t, "integrity"):
		return domain.DimConsistency
	}
	return ""
}

func dimensionFromName(name string) domain.Dimension {
	n := strings.ToLower(name)
	switch {
	case strings.Contains(n, "unique"):
		return domain.DimUniqueness
	case strings.Contains(n, "not_null"), strings.Contains(n, "not-null"), strings.Contains(n, "not null"):
		return domain.DimCompleteness
	case strings.Contains(n, "accepted_value"), strings.Contains(n, "accept"):
		return domain.DimValidity
	case strings.Contains(n, "relationship"):
		return domain.DimConsistency
	case strings.Contains(n, "recency"), strings.Contains(n, "sla"), strings.Contains(n, "fresh"):
		return domain.DimFreshness
	case strings.Contains(n, "reconcile"), strings.Contains(n, "split"), strings.Contains(n, "filter"):
		return domain.DimAccuracy
	}
	return ""
}

func stringify(v any) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	case time.Time:
		return t.UTC().Format(time.RFC3339Nano)
	case json.RawMessage:
		return string(t)
	case map[string]any, []any:
		b, err := json.Marshal(t)
		if err != nil {
			return strings.TrimSpace(fmt.Sprint(t))
		}
		return string(b)
	default:
		return strings.TrimSpace(fmt.Sprint(t))
	}
}

func asTime(v any) time.Time {
	if v == nil {
		return time.Time{}
	}
	switch t := v.(type) {
	case time.Time:
		return t.UTC()
	case *time.Time:
		if t == nil {
			return time.Time{}
		}
		return t.UTC()
	case string:
		s := strings.TrimSpace(t)
		for _, layout := range []string{
			time.RFC3339Nano, time.RFC3339,
			"2006-01-02T15:04:05.999999", "2006-01-02 15:04:05.999999 -0700",
			"2006-01-02 15:04:05.999", "2006-01-02 15:04:05",
		} {
			if parsed, err := time.Parse(layout, s); err == nil {
				return parsed.UTC()
			}
		}
		if parsed, err := time.Parse(time.RFC3339, strings.ReplaceAll(s, " ", "T")); err == nil {
			return parsed.UTC()
		}
	}
	return time.Time{}
}

func asBool(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	case string:
		s := strings.ToLower(strings.TrimSpace(t))
		return s == "true" || s == "1" || s == "yes"
	case float64:
		return t != 0
	case int:
		return t != 0
	case int64:
		return t != 0
	case json.Number:
		n, err := t.Int64()
		return err == nil && n != 0
	}
	return false
}

func recGet(rec map[string]any, keys ...string) any {
	for _, k := range keys {
		if v, ok := rec[strings.ToUpper(k)]; ok && v != nil {
			return v
		}
	}
	return nil
}

func ValidationCheck(dp domain.DataProduct, rec map[string]any) (domain.Check, bool) {
	status := MapStatus(stringify(recGet(rec, "STATUS")))
	if status == "" {
		return domain.Check{}, false
	}
	name := stringify(recGet(rec, "NAME", "CHECK_NAME"))
	if name == "" {
		name = stringify(recGet(rec, "DESCRIPTION", "CHECK_DESCRIPTION"))
	}
	if name == "" {
		name = strings.TrimSpace(stringify(recGet(rec, "TABLE_NAME")) + " " + stringify(recGet(rec, "ELEMENT")))
	}
	if name == "" {
		name = "unnamed"
	}
	runID := stringify(recGet(rec, "RUN_ID"))
	exec := asTime(recGet(rec, "RUN_TIME", "EXECUTED_AT"))
	tags := stringify(recGet(rec, "TAGS", "TAGS_RAW"))
	c := domain.Check{
		ID:              fmt.Sprintf("%s:validation:%s:%s", dp.ID, runID, name),
		DataProductID:   dp.ID,
		DataProductName: dp.Name,
		Name:            name,
		Description:     stringify(recGet(rec, "DESCRIPTION", "CHECK_DESCRIPTION")),
		Dimension:       MapDimension(stringify(recGet(rec, "DIMENSION")), tags, name),
		Severity:        MapSeverity(stringify(recGet(rec, "SEVERITY")), "validation"),
		Status:          status,
		SourceType:      domain.SrcValidation,
		SourceTable:     stringify(recGet(rec, "TABLE_NAME")),
		IsCDE:           asBool(recGet(rec, "IS_CDE")),
		Element:         stringify(recGet(rec, "ELEMENT")),
		ExecutedAt:      exec,
	}
	if status == domain.CheckFailed && !exec.IsZero() {
		t := exec
		c.FirstFailedAt = &t
	}
	return c, true
}

func DBTCheck(dp domain.DataProduct, rec map[string]any) (domain.Check, bool) {
	status := MapStatus(stringify(recGet(rec, "STATUS", "STATUS_RAW")))
	if status == "" {
		return domain.Check{}, false
	}
	name := stringify(recGet(rec, "TEST_NAME", "TEST_SHORT_NAME", "TEST_ALIAS", "NAME"))
	if name == "" {
		name = stringify(recGet(rec, "TEST_UNIQUE_ID", "UNIQUE_ID"))
	}
	if name == "" {
		return domain.Check{}, false
	}
	uniq := stringify(recGet(rec, "TEST_UNIQUE_ID", "UNIQUE_ID", "TEST_EXECUTION_ID"))
	exec := asTime(recGet(rec, "DETECTED_AT", "GENERATED_AT", "CREATED_AT"))
	tags := stringify(recGet(rec, "TAGS", "TAGS_RAW"))
	dim := MapDimension(stringify(recGet(rec, "DIMENSION", "QUALITY_DIMENSION")), tags, name)
	if dim == domain.DimUnknown {
		// Match int_dbt_test_results: unclassified dbt tests land on ACCURACY.
		dim = domain.DimAccuracy
	}
	c := domain.Check{
		ID:              fmt.Sprintf("%s:dbt:%s", dp.ID, uniq+":"+name),
		DataProductID:   dp.ID,
		DataProductName: dp.Name,
		Name:            name,
		Dimension:       dim,
		Severity:        MapSeverity(stringify(recGet(rec, "SEVERITY")), "dbt"),
		Status:          status,
		SourceType:      domain.SrcDBTTest,
		SourceTable:     stringify(recGet(rec, "TABLE_NAME", "MODEL_NAME", "SOURCE_TABLE_NAME")),
		IsCDE:           asBool(recGet(rec, "IS_CDE")),
		Element:         stringify(recGet(rec, "TEST_COLUMN_NAME", "COLUMN_NAME", "ELEMENT")),
		ExecutedAt:      exec,
	}
	if status == domain.CheckFailed && !exec.IsZero() {
		t := exec
		c.FirstFailedAt = &t
	}
	return c, true
}

// KeepLatestRun keeps rows sharing the newest RUN_ID / invocation.
func KeepLatestRun(rows []map[string]any, timeKeys, idKeys []string) []map[string]any {
	if len(rows) == 0 {
		return rows
	}
	var best time.Time
	bestID := ""
	for _, rec := range rows {
		ts := asTime(recGet(rec, timeKeys...))
		id := stringify(recGet(rec, idKeys...))
		if ts.After(best) || (ts.Equal(best) && bestID == "") {
			best = ts
			bestID = id
		}
	}
	if bestID == "" {
		return rows
	}
	out := make([]map[string]any, 0)
	for _, rec := range rows {
		if stringify(recGet(rec, idKeys...)) == bestID {
			out = append(out, rec)
		}
	}
	return out
}
