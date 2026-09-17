package astro

import (
	"net/url"
	"strings"
)

const unknownDeploymentID = ":deployment_id"

// GridURL is the Astro DAG overview stored as astro_url — same grain as
// inorbit-dbt marts.pipeline_status / macros/astro_grid_url.sql:
// {org}/{deployment_id}/dags/{dag_id}.
// Links the DAG page, not /runs/{run_id} (that route 403s under Astro DAG RBAC).
// Unknown deployment id uses the :deployment_id placeholder.
func GridURL(dep Deployment, dagID string) string {
	dagID = strings.TrimSpace(dagID)
	if dagID == "" {
		return ""
	}
	origin := strings.TrimRight(strings.TrimSpace(dep.OrgURL), "/")
	if origin == "" {
		origin = originFromAPI(dep.AirflowAPIURL)
	}
	if origin == "" {
		return ""
	}
	id := strings.Trim(strings.TrimSpace(dep.ID), "/")
	if id == "" {
		id = unknownDeploymentID
	}
	return origin + "/" + id + "/dags/" + url.PathEscape(dagID)
}

func originFromAPI(airflowURL string) string {
	u, err := url.Parse(strings.TrimSpace(airflowURL))
	if err != nil || u.Scheme == "" || u.Host == "" {
		return ""
	}
	return u.Scheme + "://" + u.Host
}

// AirflowAPIBases returns candidate Airflow REST roots.
// Same shape as astro-logs-ingestion: deployment root is {org}/{deployment_id},
// then GET {root}/api/v2/dags (Airflow 3) or {root}/api/v1/dags (Airflow 2).
func AirflowAPIBases(dep Deployment) []string {
	raw := strings.TrimRight(strings.TrimSpace(dep.AirflowAPIURL), "/")
	root := deploymentRoot(raw, dep.ID)
	seen := map[string]struct{}{}
	var out []string
	add := func(s string) {
		s = strings.TrimRight(strings.TrimSpace(s), "/")
		if s == "" {
			return
		}
		if _, ok := seen[s]; ok {
			return
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	// count_astro_dags.py uses base = {org}/{deployment_id} then GET /api/v2/dags.
	// Prefer versioned paths; keep a bare root last so httptest URLs still work.
	if versionedAPIPath(raw) && pathHasDeploymentID(raw, dep.ID) {
		add(raw)
	}
	if root != "" {
		add(root + "/api/v2")
		add(root + "/api/v1")
	}
	if !versionedAPIPath(raw) {
		add(raw)
	}
	return out
}

func versionedAPIPath(base string) bool {
	return stripAPISuffix(base) != strings.TrimRight(strings.TrimSpace(base), "/")
}

func pathHasDeploymentID(airflowURL, deploymentID string) bool {
	id := strings.Trim(strings.ToLower(strings.TrimSpace(deploymentID)), "/")
	if id == "" {
		return true
	}
	u, err := url.Parse(airflowURL)
	if err != nil {
		return false
	}
	for _, p := range strings.Split(strings.ToLower(strings.Trim(u.Path, "/")), "/") {
		if p == id {
			return true
		}
	}
	return false
}

func deploymentRoot(airflowURL, deploymentID string) string {
	base := stripAPISuffix(airflowURL)
	id := strings.Trim(strings.TrimSpace(deploymentID), "/")
	if base == "" {
		return ""
	}
	if id == "" {
		return base
	}
	if injected := injectDeploymentID(base, id); injected != "" {
		return stripAPISuffix(injected)
	}
	return base
}

func stripAPISuffix(base string) string {
	base = strings.TrimRight(strings.TrimSpace(base), "/")
	if base == "" {
		return ""
	}
	lower := strings.ToLower(base)
	for _, suffix := range []string{"/airflow/api/v1", "/airflow/api/v2", "/api/v1", "/api/v2"} {
		if strings.HasSuffix(lower, suffix) {
			return base[:len(base)-len(suffix)]
		}
	}
	return base
}

func injectDeploymentID(base, id string) string {
	id = strings.Trim(strings.TrimSpace(id), "/")
	if base == "" || id == "" {
		return ""
	}
	u, err := url.Parse(base)
	if err != nil || u.Host == "" {
		return ""
	}
	path := strings.Trim(u.Path, "/")
	if path == "" {
		u.Path = "/" + id
		return strings.TrimRight(u.String(), "/")
	}
	parts := strings.Split(path, "/")
	if parts[0] == id {
		return strings.TrimRight(base, "/")
	}
	u.Path = "/" + id + "/" + path
	return strings.TrimRight(u.String(), "/")
}

func shortErrBody(body []byte) string {
	s := strings.TrimSpace(string(body))
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.Join(strings.Fields(s), " ")
	if i := strings.Index(s, "<pre>"); i >= 0 {
		rest := s[i+5:]
		if j := strings.Index(rest, "</pre>"); j >= 0 {
			s = strings.TrimSpace(rest[:j])
		}
	}
	if len(s) > 180 {
		s = s[:180] + "…"
	}
	return s
}
