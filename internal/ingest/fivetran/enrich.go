package fivetran

import (
	"strings"

	"github.com/inorbit/inorbit/internal/domain"
)

// EnrichUpstream replaces generic fivetran_db mart nodes with one node per
// (schema, connector type) from live connections. Snowpipe / external nodes
// are left unchanged. Unmatched live groups are appended.
func EnrichUpstream(nodes []domain.LineageNode, conns []domain.Connector) []domain.LineageNode {
	groups := groupConnectors(conns)
	if len(groups) == 0 {
		return nodes
	}
	used := map[string]bool{}
	out := make([]domain.LineageNode, 0, len(nodes)+len(groups))
	for _, n := range nodes {
		if !isFivetranNode(n) {
			out = append(out, n)
			continue
		}
		matched := groupsForNode(groups, n)
		if len(matched) == 0 {
			continue
		}
		for _, g := range matched {
			if used[g.key] {
				continue
			}
			node := g.node
			node.Name = cardTitle(n, g.node.Schema)
			out = append(out, node)
			used[g.key] = true
		}
	}
	for _, g := range groups {
		if !used[g.key] {
			out = append(out, g.node)
		}
	}
	return out
}

type connGroup struct {
	key  string
	node domain.LineageNode
}

func groupConnectors(conns []domain.Connector) []connGroup {
	type key struct{ schema, service string }
	type acc struct {
		schema, service string
		n, paused       int
		failed          bool
	}
	order := make([]key, 0)
	by := map[key]*acc{}
	for _, c := range conns {
		schema := strings.TrimSpace(c.Schema)
		if schema == "" {
			schema = strings.TrimSpace(c.ID)
		}
		svc := strings.ToLower(strings.TrimSpace(c.Service))
		if svc == "" {
			svc = "fivetran"
		}
		k := key{schema: strings.ToLower(schema), service: svc}
		a := by[k]
		if a == nil {
			a = &acc{schema: schema, service: svc}
			by[k] = a
			order = append(order, k)
		}
		a.n++
		if c.Paused {
			a.paused++
		}
		if strings.EqualFold(c.Status, "FAILED") {
			a.failed = true
		}
	}
	out := make([]connGroup, 0, len(order))
	for _, k := range order {
		a := by[k]
		status := "TRUSTED"
		if a.failed {
			status = "AT_RISK"
		} else if a.paused > 0 {
			status = "CAUTION"
		}
		out = append(out, connGroup{
			key: groupKey(a.schema, a.service),
			node: domain.LineageNode{
				Name:             a.schema,
				Type:             "fivetran_db",
				Status:           status,
				Schema:           a.schema,
				ConnectorService: a.service,
				ConnectorType:    ServiceName(a.service),
				ConnectionCount:  a.n,
				PausedCount:      a.paused,
			},
		})
	}
	return out
}

func groupsForNode(groups []connGroup, n domain.LineageNode) []connGroup {
	want := canon(nodeSchema(n))
	if want == "" {
		return nil
	}
	wantTok := canon(schemaToken(nodeSchema(n)))
	var out []connGroup
	for _, g := range groups {
		got := canon(g.node.Schema)
		tok := canon(schemaToken(g.node.Schema))
		if got == want || tok == want || got == wantTok || tok == wantTok {
			out = append(out, g)
		}
	}
	return out
}

func isFivetranNode(n domain.LineageNode) bool {
	if n.ConnectorService != "" {
		return true
	}
	t := strings.ToLower(n.Type)
	return strings.Contains(t, "fivetran")
}

func cardTitle(mart domain.LineageNode, schema string) string {
	name := strings.TrimSpace(mart.Name)
	if name == "" {
		return schema
	}
	if strings.Contains(strings.ToLower(name), "fivetran_db") {
		if schema != "" {
			return schema
		}
		return nodeSchema(mart)
	}
	return name
}

func nodeSchema(n domain.LineageNode) string {
	if s := strings.TrimSpace(n.Schema); s != "" {
		return s
	}
	name := strings.TrimSpace(n.Name)
	if i := strings.LastIndex(name, "."); i >= 0 && i+1 < len(name) {
		return name[i+1:]
	}
	return name
}

func groupKey(schema, service string) string {
	return strings.ToLower(strings.TrimSpace(schema)) + "\x00" + strings.ToLower(strings.TrimSpace(service))
}

// SourceAligned reports whether a product type uses Fivetran source connectors.
func SourceAligned(t string) bool {
	return isSourceAligned(t)
}
