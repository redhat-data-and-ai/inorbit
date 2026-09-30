package pipeline

import (
	"strings"

	"github.com/inorbit/inorbit/internal/domain"
)

const (
	EnvProduction = "production"
	EnvPreprod    = "preprod"
	EnvSandbox    = "sandbox"
	EnvUnknown    = "unknown"
)

// EnvKind classifies an Airflow deployment name. Pre-prod is matched before
// prod so names like "preprod" are not treated as production.
func EnvKind(deployment string) string {
	v := strings.ToLower(strings.TrimSpace(deployment))
	if v == "" {
		return EnvUnknown
	}
	if preProdName(v) {
		return EnvPreprod
	}
	if strings.Contains(v, "prod") && !nonProdName(v) {
		return EnvProduction
	}
	if sandboxName(v) {
		return EnvSandbox
	}
	return EnvUnknown
}

func preProdName(v string) bool {
	return strings.Contains(v, "preprod") ||
		strings.Contains(v, "pre-prod") ||
		strings.Contains(v, "pre_prod") ||
		strings.Contains(v, "pre prod") ||
		strings.Contains(v, "staging") ||
		containsWord(v, "uat")
}

func nonProdName(v string) bool {
	return strings.Contains(v, "nonprod") ||
		strings.Contains(v, "non-prod") ||
		strings.Contains(v, "non_prod") ||
		strings.Contains(v, "non prod")
}

func sandboxName(v string) bool {
	return strings.Contains(v, "sandbox") ||
		containsWord(v, "sbx") ||
		containsWord(v, "dev") ||
		containsWord(v, "qa")
}

func containsWord(v, word string) bool {
	i := strings.Index(v, word)
	for i >= 0 {
		leftOK := i == 0 || !isNameChar(v[i-1])
		right := i + len(word)
		rightOK := right == len(v) || !isNameChar(v[right])
		if leftOK && rightOK {
			return true
		}
		next := strings.Index(v[i+1:], word)
		if next < 0 {
			return false
		}
		i += 1 + next
	}
	return false
}

func isNameChar(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9')
}

// HasProduction reports whether any DAG is on a production deployment.
func HasProduction(dags []domain.DAG) bool {
	for _, d := range dags {
		if EnvKind(d.DeploymentName) == EnvProduction {
			return true
		}
	}
	return false
}

// ScoreDAG is true when a DAG should contribute virtual freshness/pipeline
// checks. Custom DAGs never score. When the product has production DAGs, only
// those score so a pre-prod failure does not change production health.
func ScoreDAG(d domain.DAG, hasProd bool) bool {
	if d.IsCustom {
		return false
	}
	if hasProd {
		return EnvKind(d.DeploymentName) == EnvProduction
	}
	return true
}
