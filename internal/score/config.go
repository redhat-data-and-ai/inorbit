package score

// Config mirrors dbt_project.yml Health Score v2 vars.
type Config struct {
	TrustedThreshold          float64
	CautionThreshold          float64
	WeightFreshness           float64
	WeightAccuracy            float64
	WeightConsistency         float64
	WeightCompleteness        float64
	WeightValidity            float64
	WeightUniqueness          float64
	CoverageCapLow            float64
	CoverageCapMedium         float64
	CoverageCapNoFreshness    float64
	TimeDecaySLACriticalHours float64
	TimeDecaySLAHighHours     float64
	TimeDecaySLAMediumHours   float64
	PointsCritical            float64
	PointsHigh                float64
	PointsMedium              float64
	PointsLow                 float64
	CDEMultiplier             float64
}

func DefaultConfig() Config {
	return Config{
		TrustedThreshold:          80,
		CautionThreshold:          60,
		WeightFreshness:           25,
		WeightAccuracy:            25,
		WeightConsistency:         20,
		WeightCompleteness:        15,
		WeightValidity:            10,
		WeightUniqueness:          5,
		CoverageCapLow:            55,
		CoverageCapMedium:         70,
		CoverageCapNoFreshness:    60,
		TimeDecaySLACriticalHours: 1,
		TimeDecaySLAHighHours:     8,
		TimeDecaySLAMediumHours:   24,
		PointsCritical:            10,
		PointsHigh:                6,
		PointsMedium:              3,
		PointsLow:                 0,
		CDEMultiplier:             1.5,
	}
}
