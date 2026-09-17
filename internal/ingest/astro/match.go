package astro

import (
	"encoding/json"
	"strings"

	"github.com/inorbit/inorbit/internal/domain"
)

// Match assigns a DAG to a configured data product.
// Prefer the explicit dag_id map, then dataproduct_name, then the first
// non-system tag, then any product-named tag, then dag_id contains the
// product name (longest wins).
func Match(dagID string, tags []string, products []domain.DataProduct, extra map[string]string) (domain.DataProduct, bool) {
	return MatchDAG(dagID, tags, "", products, extra)
}

func MatchDAG(dagID string, tags []string, dataproductName string, products []domain.DataProduct, extra map[string]string) (domain.DataProduct, bool) {
	if id, ok := extra[dagID]; ok {
		if p, ok := lookupProduct(products, id); ok {
			return p, true
		}
	}
	if p, ok := lookupProduct(products, dataproductName); ok {
		return p, true
	}

	for _, tag := range tags {
		if isSystemTag(tag) {
			continue
		}
		if p, ok := lookupProduct(products, tag); ok {
			return p, true
		}
		break
	}
	for _, tag := range tags {
		if p, ok := lookupProduct(products, tag); ok {
			return p, true
		}
	}

	lowerDAG := canon(dagID)
	bestLen := 0
	var best domain.DataProduct
	for _, p := range products {
		for _, needle := range []string{canon(p.Name), canon(p.ID)} {
			if needle == "" {
				continue
			}
			if strings.Contains(lowerDAG, needle) && len(needle) > bestLen {
				best = p
				bestLen = len(needle)
			}
		}
	}
	if bestLen > 0 {
		return best, true
	}
	return domain.DataProduct{}, false
}

func lookupProduct(products []domain.DataProduct, key string) (domain.DataProduct, bool) {
	want := canon(key)
	if want == "" {
		return domain.DataProduct{}, false
	}
	for _, p := range products {
		if want == canon(p.ID) || want == canon(p.Name) {
			return p, true
		}
	}
	return domain.DataProduct{}, false
}

func canon(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.ReplaceAll(s, "-", "")
	s = strings.ReplaceAll(s, "_", "")
	s = strings.ReplaceAll(s, " ", "")
	return s
}

func isSystemTag(tag string) bool {
	t := strings.ToLower(strings.TrimSpace(tag))
	switch t {
	case "is_primary_dag", "is_custom_dag", "is_custom_dag_tag":
		return true
	}
	return strings.HasPrefix(t, "sla:")
}

func parseTagNames(raw json.RawMessage) []string {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var objs []map[string]any
	if err := json.Unmarshal(raw, &objs); err == nil {
		out := make([]string, 0, len(objs))
		for _, tag := range objs {
			if name, ok := tag["name"].(string); ok && strings.TrimSpace(name) != "" {
				out = append(out, name)
			}
		}
		if len(out) > 0 || len(objs) == 0 {
			return out
		}
	}
	var strs []string
	if err := json.Unmarshal(raw, &strs); err == nil {
		return strs
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return splitCommaTags(s)
	}
	return nil
}

func splitCommaTags(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
