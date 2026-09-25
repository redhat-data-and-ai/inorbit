package astro

import (
	"encoding/json"
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/inorbit/inorbit/internal/domain"
	"github.com/inorbit/inorbit/internal/pipeline"
)

var (
	cronEveryNMin  = regexp.MustCompile(`^\*/([0-9]+)\s+\*\s+\*\s+\*\s+\*$`)
	cronEveryNHour = regexp.MustCompile(`^[0-9]+\s+(\*/|[0-9]+-[0-9]+/)([0-9]+)\s+\*\s+\*\s+\*$`)
	cronHourly     = regexp.MustCompile(`^[0-9]+\s+\*\s+\*\s+\*\s+\*$`)
	cronMultiHour  = regexp.MustCompile(`^[0-9]+\s+[0-9]+(,[0-9]+)+\s+\*\s+\*\s+\*$`)
	cronDaily      = regexp.MustCompile(`^[0-9]+\s+[0-9]+\s+\*\s+\*\s+\*$`)
	cronWeekly     = regexp.MustCompile(`^[0-9*,/]+\s+[0-9*,/]+\s+\*\s+\*\s+[0-6]$`)
	cronMonthly    = regexp.MustCompile(`^[0-9]+\s+[0-9]+\s+[0-9]+\s+\*\s+\*$`)
	cronJSONValue  = regexp.MustCompile(`"value"\s*:\s*"([^"]+)"`)
)

// keepDAG drops stale/inactive DAGs (they 404 in Astro) and untagged paused
// DAGs. Custom, primary, and SLA-tagged paused DAGs stay visible.
func keepDAG(raw dagAPI, flags domain.DAG) bool {
	if raw.IsStale {
		return false
	}
	if raw.IsActive != nil && !*raw.IsActive {
		return false
	}
	if !raw.IsPaused {
		return true
	}
	return flags.IsCustom || flags.IsPrimary || flags.IntervalMins != nil || flags.SLAMinutes != nil
}

func applySLADefaults(d *domain.DAG, schedule string) {
	if d.IntervalMins == nil || *d.IntervalMins <= 0 {
		if v := CronIntervalMins(schedule); v != nil {
			d.IntervalMins = v
		}
	}
	pipeline.ApplySLADefaults(d)
}

// CronIntervalMins ports cron_schedule_fallback_interval_mins from inorbit-dbt.
func CronIntervalMins(raw string) *float64 {
	s := strings.TrimSpace(raw)
	if s == "" || s == "null" {
		return nil
	}
	lower := strings.ToLower(s)
	switch {
	case strings.Contains(lower, "@hourly"):
		return f64ptr(60)
	case strings.Contains(lower, "@daily"):
		return f64ptr(1440)
	case strings.Contains(lower, "@weekly"):
		return f64ptr(10080)
	case strings.Contains(lower, "@monthly"):
		return f64ptr(43200)
	}
	cron := extractCron(s)
	if cron == "" {
		return nil
	}
	if m := cronEveryNMin.FindStringSubmatch(cron); len(m) == 2 {
		return parsePositiveMins(m[1])
	}
	if m := cronEveryNHour.FindStringSubmatch(cron); len(m) == 3 {
		n, err := strconv.Atoi(m[2])
		if err != nil || n <= 0 {
			return nil
		}
		return f64ptr(float64(n * 60))
	}
	if cronHourly.MatchString(cron) {
		return f64ptr(60)
	}
	if cronMultiHour.MatchString(cron) {
		n := strings.Count(cron, ",") + 1
		return f64ptr(math.Round(1440.0 / float64(n)))
	}
	if cronDaily.MatchString(cron) {
		return f64ptr(1440)
	}
	if cronWeekly.MatchString(cron) {
		return f64ptr(10080)
	}
	if cronMonthly.MatchString(cron) {
		return f64ptr(43200)
	}
	return nil
}

func extractCron(raw string) string {
	if m := cronJSONValue.FindStringSubmatch(raw); len(m) == 2 {
		return strings.TrimSpace(m[1])
	}
	s := strings.TrimSpace(raw)
	s = strings.Trim(s, `"'`)
	return s
}

func (d dagAPI) scheduleText() string {
	if s := strings.TrimSpace(d.TimetableSummary); s != "" {
		return s
	}
	if len(d.ScheduleInterval) == 0 || string(d.ScheduleInterval) == "null" {
		return ""
	}
	var asString string
	if err := json.Unmarshal(d.ScheduleInterval, &asString); err == nil {
		return asString
	}
	return strings.TrimSpace(string(d.ScheduleInterval))
}

func parsePositiveMins(s string) *float64 {
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return nil
	}
	return f64ptr(float64(n))
}

func f64ptr(v float64) *float64 { return &v }
