package lineage

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/inorbit/inorbit/internal/domain"
)

// Map turns warehouse DP_LINEAGE rows into one Lineage record per configured product.
func Map(rows []map[string]any, products []domain.DataProduct) []domain.Lineage {
	byKey := map[string]domain.Lineage{}
	for _, rec := range rows {
		lin, ok := fromRow(rec)
		if !ok {
			continue
		}
		for _, k := range []string{canon(lin.DataProductID), canon(lin.DataProductName)} {
			if k != "" {
				byKey[k] = lin
			}
		}
	}
	out := make([]domain.Lineage, 0, len(products))
	for _, p := range products {
		lin, ok := byKey[canon(p.ID)]
		if !ok {
			lin, ok = byKey[canon(p.Name)]
		}
		if !ok {
			lin = domain.Lineage{}
		}
		lin.DataProductID = p.ID
		lin.DataProductName = p.Name
		if lin.UpstreamSources == nil {
			lin.UpstreamSources = []domain.LineageNode{}
		}
		if lin.DownstreamConsumers == nil {
			lin.DownstreamConsumers = []domain.LineageNode{}
		}
		if lin.UpstreamCount == 0 {
			lin.UpstreamCount = len(lin.UpstreamSources)
		}
		if lin.DirectDownstreamCount == 0 {
			lin.DirectDownstreamCount = len(lin.DownstreamConsumers)
		}
		out = append(out, lin)
	}
	return out
}

func fromRow(rec map[string]any) (domain.Lineage, bool) {
	id := str(rec, "DATA_PRODUCT_ID", "data_product_id")
	name := str(rec, "DATA_PRODUCT_NAME", "data_product_name")
	if id == "" && name == "" {
		return domain.Lineage{}, false
	}
	up := parseNodes(first(rec, "UPSTREAM_SOURCES", "upstream_sources"))
	down := parseNodes(first(rec, "DOWNSTREAM_CONSUMERS", "downstream_consumers"))
	lin := domain.Lineage{
		DataProductID:         strings.ToLower(strings.TrimSpace(id)),
		DataProductName:       strings.ToLower(strings.TrimSpace(name)),
		Status:                str(rec, "STATUS", "status"),
		HealthScore:           floatPtr(first(rec, "HEALTH_SCORE", "health_score")),
		UpstreamSources:       up,
		UpstreamCount:         asInt(first(rec, "UPSTREAM_COUNT", "upstream_count"), len(up)),
		DownstreamConsumers:   down,
		DirectDownstreamCount: asInt(first(rec, "DIRECT_DOWNSTREAM_COUNT", "direct_downstream_count"), len(down)),
		DirectDPConsumerCount: asInt(first(rec, "DIRECT_DP_CONSUMER_COUNT", "direct_dp_consumer_count"), 0),
		ServiceAccountCount:   asInt(first(rec, "SERVICE_ACCOUNT_COUNT", "service_account_count"), 0),
		ConsumerGroupCount:    asInt(first(rec, "CONSUMER_GROUP_COUNT", "consumer_group_count"), 0),
		BlastRadiusCount:      asInt(first(rec, "BLAST_RADIUS_COUNT", "blast_radius_count"), 0),
		BlastRadiusScore:      str(rec, "BLAST_RADIUS_SCORE", "blast_radius_score"),
		ComputedAt:            asTime(first(rec, "COMPUTED_AT", "computed_at")),
	}
	if lin.DataProductID == "" {
		lin.DataProductID = lin.DataProductName
	}
	return lin, true
}

func parseNodes(v any) []domain.LineageNode {
	if v == nil {
		return []domain.LineageNode{}
	}
	var raw []any
	switch t := v.(type) {
	case []domain.LineageNode:
		return t
	case []any:
		raw = t
	case string:
		s := strings.TrimSpace(t)
		if s == "" || s == "null" {
			return []domain.LineageNode{}
		}
		if err := json.Unmarshal([]byte(s), &raw); err != nil {
			var nodes []domain.LineageNode
			if err2 := json.Unmarshal([]byte(s), &nodes); err2 == nil {
				return nodes
			}
			return []domain.LineageNode{}
		}
	case []byte:
		if err := json.Unmarshal(t, &raw); err != nil {
			var nodes []domain.LineageNode
			if err2 := json.Unmarshal(t, &nodes); err2 == nil {
				return nodes
			}
			return []domain.LineageNode{}
		}
	default:
		b, err := json.Marshal(t)
		if err != nil {
			return []domain.LineageNode{}
		}
		if err := json.Unmarshal(b, &raw); err != nil {
			var nodes []domain.LineageNode
			if err2 := json.Unmarshal(b, &nodes); err2 == nil {
				return nodes
			}
			return []domain.LineageNode{}
		}
	}
	out := make([]domain.LineageNode, 0, len(raw))
	for _, item := range raw {
		n, ok := nodeFrom(item)
		if ok {
			out = append(out, n)
		}
	}
	return out
}

func nodeFrom(v any) (domain.LineageNode, bool) {
	switch t := v.(type) {
	case domain.LineageNode:
		return t, t.Name != ""
	case map[string]any:
		name := str(t, "name", "NAME")
		if name == "" {
			return domain.LineageNode{}, false
		}
		return domain.LineageNode{
			Name:        name,
			Type:        str(t, "type", "TYPE"),
			Status:      str(t, "status", "STATUS"),
			HealthScore: floatPtr(first(t, "health_score", "HEALTH_SCORE")),
		}, true
	default:
		b, err := json.Marshal(t)
		if err != nil {
			return domain.LineageNode{}, false
		}
		var m map[string]any
		if err := json.Unmarshal(b, &m); err != nil {
			return domain.LineageNode{}, false
		}
		return nodeFrom(m)
	}
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
		return strings.TrimSpace(strings.Trim(asString(t), `"`))
	}
}

func asString(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
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
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05.000", "2006-01-02 15:04:05"} {
			if parsed, err := time.Parse(layout, strings.TrimSpace(t)); err == nil {
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
