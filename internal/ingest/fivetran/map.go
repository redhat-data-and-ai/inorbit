package fivetran

import (
	"strings"
	"time"

	"github.com/inorbit/inorbit/internal/domain"
	"github.com/inorbit/inorbit/internal/pipeline"
)

func toDAG(p domain.DataProduct, group groupAPI, conn connectionAPI, dashboardBase string, now time.Time) domain.DAG {
	return AsDAG(toConnector(p, group, conn, dashboardBase, now))
}

func toConnector(p domain.DataProduct, group groupAPI, conn connectionAPI, dashboardBase string, now time.Time) domain.Connector {
	succeeded := parseTS(conn.SucceededAt)
	failed := parseTS(conn.FailedAt)
	status, errMsg := connectionStatus(conn, succeeded, failed)
	interval := intervalMins(conn)
	completed := succeeded
	if status == "FAILED" && failed != nil && (completed == nil || failed.After(*completed)) {
		completed = failed
	}
	c := domain.Connector{
		DataProductID:   p.ID,
		DataProductName: p.Name,
		ID:              conn.ID,
		Schema:          strings.TrimSpace(conn.Schema),
		Service:         strings.TrimSpace(conn.Service),
		ServiceName:     ServiceName(conn.Service),
		GroupName:       group.Name,
		GroupID:         group.ID,
		Paused:          conn.Paused || strings.EqualFold(conn.Status.SyncState, "paused"),
		Status:          status,
		ErrorMessage:    errMsg,
		DashboardURL:    dashboardURL(dashboardBase, conn.ID),
		SucceededAt:     succeeded,
		FailedAt:        failed,
		CompletedAt:     completed,
		IntervalMins:    interval,
		TriggerType:     triggerType(conn.ScheduleType),
	}
	if c.GroupName == "" {
		c.GroupName = group.ID
	}
	if c.Schema == "" {
		c.Schema = displayID(conn)
	}
	if status == "RUNNING" {
		c.StartedAt = &now
	} else if completed != nil {
		c.StartedAt = completed
	}
	d := AsDAG(c)
	c.SLAMinutes = d.SLAMinutes
	c.NextExpectedAt = d.NextExpectedAt
	return c
}

// AsDAG is the scoring view of a connector. It is not shown as a Pipeline row.
func AsDAG(c domain.Connector) domain.DAG {
	schema := strings.TrimSpace(c.Schema)
	if schema == "" {
		schema = strings.TrimSpace(c.ID)
	}
	d := domain.DAG{
		DataProductID:   c.DataProductID,
		DataProductName: c.DataProductName,
		DeploymentName:  c.GroupName,
		DeploymentID:    c.GroupID,
		DAGID:           schema,
		RunID:           c.ID,
		Status:          c.Status,
		StartedAt:       c.StartedAt,
		CompletedAt:     c.CompletedAt,
		ErrorMessage:    c.ErrorMessage,
		IsPaused:        c.Paused,
		SilentMonitored: c.IntervalMins != nil,
		IntervalMins:    c.IntervalMins,
		SLAMinutes:      c.SLAMinutes,
		NextExpectedAt:  c.NextExpectedAt,
		AstroURL:        c.DashboardURL,
		TriggerType:     c.TriggerType,
		PipelineType:    domain.PipelineTypeFivetran,
		LastSuccessAt:   c.SucceededAt,
	}
	if d.DeploymentName == "" {
		d.DeploymentName = c.GroupID
	}
	if d.LastSuccessAt != nil && d.IntervalMins != nil && d.NextExpectedAt == nil {
		next := d.LastSuccessAt.Add(time.Duration(*d.IntervalMins) * time.Minute)
		d.NextExpectedAt = &next
	}
	pipeline.ApplySLADefaults(&d)
	d.FrequencyDisplay = pipeline.FrequencyDisplay(d.IntervalMins)
	if d.FrequencyDisplay == "" && strings.EqualFold(c.TriggerType, "MANUAL") {
		d.FrequencyDisplay = "Manual"
	}
	return d
}

// AsDAGs converts connectors for freshness/pipeline virtual checks.
func AsDAGs(conns []domain.Connector) []domain.DAG {
	out := make([]domain.DAG, 0, len(conns))
	for _, c := range conns {
		out = append(out, AsDAG(c))
	}
	markPrimary(out)
	return out
}

func displayID(conn connectionAPI) string {
	if s := strings.TrimSpace(conn.Schema); s != "" {
		return s
	}
	if s := strings.TrimSpace(conn.Service); s != "" {
		return s
	}
	return conn.ID
}

func intervalMins(conn connectionAPI) *float64 {
	if conn.SyncFrequency <= 0 || strings.EqualFold(conn.ScheduleType, "manual") {
		return nil
	}
	v := float64(conn.SyncFrequency)
	return &v
}

func triggerType(schedule string) string {
	switch strings.ToLower(strings.TrimSpace(schedule)) {
	case "manual":
		return "MANUAL"
	case "auto", "":
		return "SCHEDULED"
	default:
		return strings.ToUpper(schedule)
	}
}

func dashboardURL(base, id string) string {
	id = strings.TrimSpace(id)
	if id == "" {
		return ""
	}
	base = strings.TrimRight(strings.TrimSpace(base), "/")
	if base == "" {
		base = defaultDashboardURL
	}
	return base + "/" + id
}

func connectionStatus(conn connectionAPI, succeeded, failed *time.Time) (string, string) {
	errMsg := firstMessage(conn.Status.Tasks)
	if errMsg == "" {
		errMsg = firstMessage(conn.Status.Warnings)
	}
	setup := strings.ToLower(conn.Status.SetupState)
	if setup == "broken" {
		if errMsg == "" {
			errMsg = "Fivetran setup is broken"
		}
		return "FAILED", errMsg
	}
	if len(conn.Status.Tasks) > 0 {
		if errMsg == "" {
			errMsg = "Fivetran connector is unhealthy"
		}
		return "FAILED", errMsg
	}
	if failed != nil && (succeeded == nil || failed.After(*succeeded)) {
		if errMsg == "" {
			errMsg = "Last Fivetran sync failed"
		}
		return "FAILED", errMsg
	}
	if setup == "incomplete" && succeeded == nil {
		if errMsg == "" {
			errMsg = "Fivetran setup is incomplete"
		}
		return "FAILED", errMsg
	}
	sync := strings.ToLower(conn.Status.SyncState)
	if sync == "syncing" || conn.Status.IsHistoricalSync {
		return "RUNNING", errMsg
	}
	return "SUCCESS", errMsg
}

func firstMessage(rows []statusNote) string {
	for _, row := range rows {
		if s := strings.TrimSpace(row.Message); s != "" {
			return s
		}
	}
	return ""
}

func parseTS(s string) *time.Time {
	s = strings.TrimSpace(s)
	if s == "" || strings.EqualFold(s, "null") {
		return nil
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if t, err := time.Parse(layout, s); err == nil {
			u := t.UTC()
			return &u
		}
	}
	return nil
}

func markPrimary(dags []domain.DAG) {
	type key struct{ product, dep string }
	best := map[key]int{}
	for i, d := range dags {
		k := key{d.DataProductID, strings.ToLower(d.DeploymentName)}
		prev, ok := best[k]
		if !ok || primaryRank(dags[i]) > primaryRank(dags[prev]) {
			best[k] = i
		}
	}
	for _, i := range best {
		dags[i].IsPrimary = true
	}
}

func primaryRank(d domain.DAG) int {
	id := canon(d.DataProductID)
	name := canon(d.DataProductName)
	got := canon(d.DAGID)
	switch {
	case got != "" && (got == id || got == name):
		return 3
	case strings.Contains(got, id) || (name != "" && strings.Contains(got, name)):
		return 2
	default:
		return 1
	}
}

func canon(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.ReplaceAll(s, "-", "")
	s = strings.ReplaceAll(s, "_", "")
	s = strings.ReplaceAll(s, " ", "")
	return s
}
