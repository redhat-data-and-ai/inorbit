package score

import (
	"fmt"
	"math"
	"strings"

	"github.com/inorbit/inorbit/internal/domain"
)

type dimAgg struct {
	deduction float64
	checks    int
	failed    int
	warning   int
}

// Evaluate ports int_health_score.sql for the given products and checks.
func Evaluate(products []domain.DataProduct, checks []domain.Check, cfg Config) []domain.HealthStatus {
	byDP := map[string][]domain.Check{}
	for _, c := range checks {
		byDP[c.DataProductID] = append(byDP[c.DataProductID], c)
	}
	out := make([]domain.HealthStatus, 0, len(products))
	for _, dp := range products {
		out = append(out, scoreOne(dp, byDP[dp.ID], cfg))
	}
	return out
}

func scoreOne(dp domain.DataProduct, checks []domain.Check, cfg Config) domain.HealthStatus {
	h := domain.HealthStatus{
		DataProductID:   dp.ID,
		DataProductName: dp.Name,
	}
	if len(checks) == 0 {
		h.Status = domain.HealthAtRisk
		h.StatusMessage = "No checks configured"
		return h
	}

	agg := map[domain.Dimension]*dimAgg{}
	var astro, validation int
	for _, c := range checks {
		h.TotalChecks++
		if c.Status == domain.CheckFailed {
			h.FailedChecks++
		}
		if c.Status == domain.CheckWarning {
			h.WarningChecks++
		}
		switch c.SourceType {
		case domain.SrcAstroFreshness, domain.SrcAstroPipeline:
			astro++
		case domain.SrcValidation:
			validation++
		}
		dim := domain.Dimension(strings.ToUpper(string(c.Dimension)))
		if dim == "" || dim == domain.DimUnknown {
			continue
		}
		a := agg[dim]
		if a == nil {
			a = &dimAgg{}
			agg[dim] = a
		}
		a.checks++
		a.deduction += c.FinalDeduction
		if c.Status == domain.CheckFailed {
			a.failed++
		}
		if c.Status == domain.CheckWarning {
			a.warning++
		}
	}
	h.AstroCheckCount = astro
	h.ValidationCheckCount = validation

	var weightSum float64
	scores := map[domain.Dimension]float64{}
	for dim, a := range agg {
		w := Weight(dim, cfg)
		if w == 0 {
			continue
		}
		scores[dim] = math.Max(0, 100-a.deduction)
		weightSum += w
	}
	if weightSum == 0 {
		h.Status = domain.HealthAtRisk
		h.StatusMessage = "No checks configured"
		return h
	}

	var raw float64
	for dim, sc := range scores {
		raw += sc * (Weight(dim, cfg) / weightSum)
	}
	measured := len(scores)
	_, freshnessMeasured := scores[domain.DimFreshness]
	capped := raw
	capApplied := false
	switch {
	case raw >= 100:
		capped = 100
	case measured <= 2:
		capped = math.Min(raw, cfg.CoverageCapLow)
		capApplied = true
	case measured <= 3:
		capped = math.Min(raw, cfg.CoverageCapMedium)
		capApplied = true
	case !freshnessMeasured:
		capped = math.Min(raw, cfg.CoverageCapNoFreshness)
		capApplied = true
	}
	if raw >= 100 {
		capApplied = false
		capped = 100
	}

	h.HealthScore = capped
	h.MeasuredDimensionCount = measured
	h.CoveragePct = math.Round(float64(measured)/6.0*10000) / 100
	h.CoverageCapApplied = capApplied
	h.FreshnessScore = ptrScore(scores, domain.DimFreshness)
	h.AccuracyScore = ptrScore(scores, domain.DimAccuracy)
	h.ConsistencyScore = ptrScore(scores, domain.DimConsistency)
	h.CompletenessScore = ptrScore(scores, domain.DimCompleteness)
	h.ValidityScore = ptrScore(scores, domain.DimValidity)
	h.UniquenessScore = ptrScore(scores, domain.DimUniqueness)

	switch {
	case h.HealthScore >= cfg.TrustedThreshold:
		h.Status = domain.HealthTrusted
	case h.HealthScore >= cfg.CautionThreshold:
		h.Status = domain.HealthCaution
	default:
		h.Status = domain.HealthAtRisk
	}
	h.StatusMessage = statusMessage(h, measured)
	return h
}

func ptrScore(scores map[domain.Dimension]float64, dim domain.Dimension) *float64 {
	v, ok := scores[dim]
	if !ok {
		return nil
	}
	x := v
	return &x
}

func statusMessage(h domain.HealthStatus, measured int) string {
	if h.CoverageCapApplied {
		return fmt.Sprintf("Score capped at %.2f (coverage: %d/6 dimensions measured)", h.HealthScore, measured)
	}
	if h.FailedChecks == 0 {
		return "All checks passing"
	}
	return fmt.Sprintf("%d failed check(s) across %d dimension(s)", h.FailedChecks, measured)
}
