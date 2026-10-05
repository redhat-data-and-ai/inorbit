package fivetran

import (
	"strings"

	"github.com/inorbit/inorbit/internal/domain"
	"github.com/inorbit/inorbit/internal/ingest/astro"
)

// Match assigns a Fivetran connection to a catalog product.
// Explicit connector ids win. Auto-match only considers source-aligned
// products, using schema (and its first path token), then service name.
func Match(connectorID, schema, service string, products []domain.DataProduct, extra map[string]string) (domain.DataProduct, bool) {
	if p, ok := astro.MatchDAG(connectorID, nil, "", products, extra); ok {
		return p, true
	}
	aligned := sourceAligned(products)
	if len(aligned) == 0 {
		return domain.DataProduct{}, false
	}
	if p, ok := astro.MatchDAG(schema, nil, "", aligned, nil); ok {
		return p, true
	}
	if tok := schemaToken(schema); tok != "" && !strings.EqualFold(tok, schema) {
		if p, ok := astro.MatchDAG(tok, nil, "", aligned, nil); ok {
			return p, true
		}
	}
	if p, ok := astro.MatchDAG(service, nil, service, aligned, nil); ok {
		return p, true
	}
	return domain.DataProduct{}, false
}

func sourceAligned(products []domain.DataProduct) []domain.DataProduct {
	out := make([]domain.DataProduct, 0, len(products))
	for _, p := range products {
		if isSourceAligned(p.Type) {
			out = append(out, p)
		}
	}
	return out
}

func isSourceAligned(t string) bool {
	v := strings.ToLower(strings.TrimSpace(t))
	return v == "source-aligned" || v == "source"
}

func schemaToken(schema string) string {
	schema = strings.TrimSpace(schema)
	if schema == "" {
		return ""
	}
	if i := strings.IndexAny(schema, "."); i > 0 {
		return schema[:i]
	}
	return schema
}
