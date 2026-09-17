package score

import (
	"strings"
	"time"

	"github.com/inorbit/inorbit/internal/domain"
)

func Weight(dim domain.Dimension, cfg Config) float64 {
	switch domain.Dimension(strings.ToUpper(string(dim))) {
	case domain.DimFreshness:
		return cfg.WeightFreshness
	case domain.DimAccuracy:
		return cfg.WeightAccuracy
	case domain.DimConsistency:
		return cfg.WeightConsistency
	case domain.DimCompleteness:
		return cfg.WeightCompleteness
	case domain.DimValidity:
		return cfg.WeightValidity
	case domain.DimUniqueness:
		return cfg.WeightUniqueness
	default:
		return 0
	}
}

func severityPoints(sev domain.Severity, cfg Config) float64 {
	switch domain.Severity(strings.ToUpper(string(sev))) {
	case domain.SevCritical:
		return cfg.PointsCritical
	case domain.SevHigh:
		return cfg.PointsHigh
	case domain.SevMedium:
		return cfg.PointsMedium
	default:
		return cfg.PointsLow
	}
}

// BasePoints ports macros/get_base_points.sql.
func BasePoints(sev domain.Severity, status domain.CheckStatus, flapping bool, cfg Config) float64 {
	st := domain.CheckStatus(strings.ToUpper(string(status)))
	if flapping && st == domain.CheckPassed {
		return cfg.PointsMedium
	}
	pts := severityPoints(sev, cfg)
	if flapping && st == domain.CheckFailed {
		if pts < cfg.PointsMedium {
			return cfg.PointsMedium
		}
		return pts
	}
	if st == domain.CheckFailed {
		return pts
	}
	return 0
}

// TimeDecay ports macros/get_time_decay.sql (whole hours elapsed).
func TimeDecay(sev domain.Severity, status domain.CheckStatus, firstFailed *time.Time, asOf time.Time, cfg Config) float64 {
	if firstFailed == nil || domain.CheckStatus(strings.ToUpper(string(status))) != domain.CheckFailed {
		return 1
	}
	hours := asOf.UTC().Sub(firstFailed.UTC()).Hours()
	switch domain.Severity(strings.ToUpper(string(sev))) {
	case domain.SevCritical:
		if hours > cfg.TimeDecaySLACriticalHours*2 {
			return 2
		}
		if hours > cfg.TimeDecaySLACriticalHours {
			return 1.5
		}
	case domain.SevHigh:
		if hours > cfg.TimeDecaySLAHighHours*2 {
			return 2
		}
		if hours > cfg.TimeDecaySLAHighHours {
			return 1.5
		}
	case domain.SevMedium:
		if hours > cfg.TimeDecaySLAMediumHours*2 {
			return 1.5
		}
		if hours > cfg.TimeDecaySLAMediumHours {
			return 1.25
		}
	}
	return 1
}

// Apply fills deduction columns. Matches int_unified_checks: only FAILED deducts.
func Apply(c domain.Check, asOf time.Time, cfg Config) domain.Check {
	if c.BlastRadiusMultiplier == 0 {
		c.BlastRadiusMultiplier = 1
	}
	c.BasePoints = BasePoints(c.Severity, c.Status, c.IsFlapping, cfg)
	if c.IsCDE {
		c.CDEMultiplier = cfg.CDEMultiplier
	} else {
		c.CDEMultiplier = 1
	}
	c.TimeDecayMultiplier = TimeDecay(c.Severity, c.Status, c.FirstFailedAt, asOf, cfg)
	if domain.CheckStatus(strings.ToUpper(string(c.Status))) == domain.CheckFailed {
		c.FinalDeduction = c.BasePoints * c.BlastRadiusMultiplier * c.CDEMultiplier * c.TimeDecayMultiplier
	} else {
		c.FinalDeduction = 0
	}
	c.SourceDisplayName = displayName(c.SourceType)
	return c
}

func displayName(src domain.SourceType) string {
	switch src {
	case domain.SrcValidX:
		return "Validation"
	case domain.SrcDBTTest:
		return "dbt / Elementary"
	case domain.SrcAstroFreshness, domain.SrcAstroPipeline:
		return "Astro"
	default:
		return string(src)
	}
}
