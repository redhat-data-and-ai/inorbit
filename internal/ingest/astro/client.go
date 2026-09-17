package astro

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/inorbit/inorbit/internal/domain"
	"github.com/inorbit/inorbit/internal/pipeline"
)

type Deployment struct {
	Name          string
	ID            string
	AirflowAPIURL string
	OrgURL        string
}

type Client struct {
	HTTP        *http.Client
	Token       string
	Deployments []Deployment
	Log         *log.Logger
	resolved    sync.Map // deployment name → working Airflow API base
	tagCache    sync.Map // deployment/dag_id → tags JSON
}

type dagAPI struct {
	DAGID            string          `json:"dag_id"`
	IsPaused         bool            `json:"is_paused"`
	IsStale          bool            `json:"is_stale"`
	IsActive         *bool           `json:"is_active"`
	Tags             json.RawMessage `json:"tags"`
	NextDagRun       string          `json:"next_dagrun"`
	NextDagRunAlt    string          `json:"next_dag_run"`
	DataproductName  string          `json:"dataproduct_name"`
	ScheduleInterval json.RawMessage `json:"schedule_interval"`
	TimetableSummary string          `json:"timetable_summary"`
}

type dagRunAPI struct {
	DAGRunID  string `json:"dag_run_id"`
	State     string `json:"state"`
	RunType   string `json:"run_type"`
	StartDate string `json:"start_date"`
	EndDate   string `json:"end_date"`
	Note      string `json:"note"`
}

const (
	runPageSize = 100
	runMaxPages = 4
)

func (c *Client) http() *http.Client {
	if c.HTTP == nil {
		c.HTTP = &http.Client{Timeout: 60 * time.Second}
	}
	return c.HTTP
}

// FetchLatest pulls DAG tags + latest run per DAG for one deployment,
// keeping only DAGs present in dataProductByDAG.
func (c *Client) FetchLatest(ctx context.Context, dep Deployment, dataProductByDAG map[string]domain.DataProduct) ([]domain.DAG, error) {
	products := make([]domain.DataProduct, 0, len(dataProductByDAG))
	seen := map[string]struct{}{}
	extra := map[string]string{}
	for dagID, p := range dataProductByDAG {
		extra[dagID] = p.ID
		if _, ok := seen[p.ID]; ok {
			continue
		}
		seen[p.ID] = struct{}{}
		products = append(products, p)
	}
	return c.fetchDeployment(ctx, dep, products, extra)
}

// FetchForProducts lists DAGs on every deployment and keeps those that match
// configured data products (explicit dag_id, tag, or name in dag_id).
func (c *Client) FetchForProducts(ctx context.Context, products []domain.DataProduct, extra map[string]string) ([]domain.DAG, error) {
	var out []domain.DAG
	var last error
	for _, dep := range c.Deployments {
		if strings.TrimSpace(dep.AirflowAPIURL) == "" && dep.ID == "" {
			continue
		}
		dags, err := c.fetchDeployment(ctx, dep, products, extra)
		if err != nil {
			last = err
			c.logf("airflow %s: %v", dep.Name, err)
			continue
		}
		out = append(out, dags...)
	}
	if len(out) == 0 && last != nil {
		return nil, last
	}
	return out, nil
}

func (c *Client) logf(format string, args ...any) {
	if c.Log != nil {
		c.Log.Printf(format, args...)
	}
}

func (c *Client) fetchDeployment(ctx context.Context, dep Deployment, products []domain.DataProduct, extra map[string]string) ([]domain.DAG, error) {
	listed, err := c.listDAGs(ctx, dep)
	if err != nil {
		return nil, err
	}
	c.enrichTags(ctx, dep, listed)

	type matched struct {
		raw     dagAPI
		product domain.DataProduct
	}
	keep := make([]matched, 0)
	withTags := 0
	skipped := 0
	var unmatched []string
	var matchedIDs []string
	for _, raw := range listed {
		tags := parseTagNames(raw.Tags)
		if len(tags) > 0 {
			withTags++
		}
		product, ok := MatchDAG(raw.DAGID, tags, raw.DataproductName, products, extra)
		if !ok {
			if len(unmatched) < 12 {
				unmatched = append(unmatched, raw.DAGID)
			}
			continue
		}
		var flags domain.DAG
		flags.DAGID = raw.DAGID
		parseTags(&flags, tags)
		inferCustomFromID(&flags)
		if !keepDAG(raw, flags) {
			skipped++
			continue
		}
		if len(matchedIDs) < 12 {
			matchedIDs = append(matchedIDs, raw.DAGID)
		}
		keep = append(keep, matched{raw: raw, product: product})
	}
	c.logf("airflow %s: listed=%d with_tags=%d matched=%d skipped_inactive=%d unmatched=%d kept=%s dropped=%s",
		dep.Name, len(listed), withTags, len(keep), skipped, len(listed)-len(keep)-skipped, strings.Join(matchedIDs, ","), strings.Join(unmatched, ","))

	out := make([]domain.DAG, len(keep))
	var wg sync.WaitGroup
	sem := make(chan struct{}, 8)
	for i, item := range keep {
		wg.Add(1)
		go func(i int, item matched) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			runs, _ := c.listRuns(ctx, dep, item.raw.DAGID, "", runPageSize, runMaxPages)
			var run *dagRunAPI
			if len(runs) > 0 {
				run = &runs[0]
			}
			var lastOK *dagRunAPI
			for ri := range runs {
				if strings.EqualFold(runs[ri].State, "success") {
					lastOK = &runs[ri]
					break
				}
			}
			if lastOK == nil {
				lastOK, _ = c.latestRun(ctx, dep, item.raw.DAGID, "success")
			}
			out[i] = normalize(dep, item.product, item.raw, run, lastOK)
			pipeline.ApplyReliability(&out[i], runFacts(runs), time.Now().UTC())
			pipeline.EnrichDAG(&out[i])
		}(i, item)
	}
	wg.Wait()
	kept := out[:0]
	for _, d := range out {
		if d.DAGID == "" || !dagAvailable(d) {
			continue
		}
		kept = append(kept, d)
	}
	return kept, nil
}

func dagAvailable(d domain.DAG) bool {
	if d.IsCustom {
		return true
	}
	if strings.TrimSpace(d.Status) != "" {
		return true
	}
	if d.NextExpectedAt != nil || d.LastSuccessAt != nil || d.StartedAt != nil || d.CompletedAt != nil {
		return true
	}
	return false
}

func (c *Client) apiBase(dep Deployment) string {
	if v, ok := c.resolved.Load(dep.Name); ok {
		if s, _ := v.(string); s != "" {
			return s
		}
	}
	return strings.TrimRight(dep.AirflowAPIURL, "/")
}

func (c *Client) listDAGs(ctx context.Context, dep Deployment) ([]dagAPI, error) {
	bases := AirflowAPIBases(dep)
	if v, ok := c.resolved.Load(dep.Name); ok {
		if s, _ := v.(string); s != "" {
			bases = []string{s}
		}
	}
	var last error
	for _, base := range bases {
		dags, err := c.listDAGsFrom(ctx, dep.Name, base)
		if err == nil {
			c.resolved.Store(dep.Name, base)
			return dags, nil
		}
		last = err
	}
	if last == nil {
		last = fmt.Errorf("list dags %s: no airflow_api_url", dep.Name)
	}
	return nil, last
}

func (c *Client) listDAGsFrom(ctx context.Context, depName, base string) ([]dagAPI, error) {
	dags, err := c.listDAGsPaged(ctx, depName, base, false)
	if err != nil && excludeStaleUnsupported(err) {
		return c.listDAGsPaged(ctx, depName, base, true)
	}
	return dags, err
}

func excludeStaleUnsupported(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, " 400 ") || strings.Contains(s, "400 Bad Request") || strings.Contains(s, " 422 ")
}

func (c *Client) listDAGsPaged(ctx context.Context, depName, base string, includeStale bool) ([]dagAPI, error) {
	const page = 100
	var all []dagAPI
	for offset := 0; ; offset += page {
		u := fmt.Sprintf("%s/dags?limit=%d&offset=%d", strings.TrimRight(base, "/"), page, offset)
		if includeStale {
			u += "&exclude_stale=false"
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return nil, err
		}
		c.auth(req)
		resp, err := c.http().Do(req)
		if err != nil {
			return nil, err
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
		resp.Body.Close()
		if resp.StatusCode >= 300 {
			return nil, fmt.Errorf("list dags %s: %s %s (%s)", depName, resp.Status, shortErrBody(body), base)
		}
		ct := strings.ToLower(resp.Header.Get("Content-Type"))
		if strings.Contains(ct, "text/html") || (len(body) > 0 && body[0] == '<') {
			return nil, fmt.Errorf("list dags %s: not an Airflow API (%s)", depName, base)
		}
		var payload struct {
			DAGs         []dagAPI `json:"dags"`
			TotalEntries *int     `json:"total_entries"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			return nil, fmt.Errorf("list dags %s: decode %w (%s)", depName, err, base)
		}
		batch := payload.DAGs
		all = append(all, batch...)
		// Same stop rule as astro_logs_ingestion.AstroAPIClient.list_all_dags.
		if payload.TotalEntries != nil && offset+len(batch) >= *payload.TotalEntries {
			break
		}
		if len(batch) < page {
			break
		}
	}
	return all, nil
}

func (c *Client) enrichTags(ctx context.Context, dep Deployment, listed []dagAPI) {
	var wg sync.WaitGroup
	sem := make(chan struct{}, 8)
	for i := range listed {
		if len(parseTagNames(listed[i].Tags)) > 0 || strings.TrimSpace(listed[i].DataproductName) != "" {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if filled, err := c.fillDAGTags(ctx, dep, listed[i]); err == nil {
				listed[i] = filled
			}
		}(i)
	}
	wg.Wait()
}

func (c *Client) fillDAGTags(ctx context.Context, dep Deployment, raw dagAPI) (dagAPI, error) {
	key := dep.Name + "/" + raw.DAGID
	if v, ok := c.tagCache.Load(key); ok {
		if cached, ok := v.(json.RawMessage); ok && len(cached) > 0 {
			raw.Tags = cached
			return raw, nil
		}
	}
	got, err := c.getDAG(ctx, dep, raw.DAGID)
	if err != nil {
		return raw, err
	}
	if len(got.Tags) > 0 {
		c.tagCache.Store(key, got.Tags)
		raw.Tags = got.Tags
	}
	if got.DataproductName != "" {
		raw.DataproductName = got.DataproductName
	}
	if raw.NextDagRun == "" && raw.NextDagRunAlt == "" {
		raw.NextDagRun = got.NextDagRun
		raw.NextDagRunAlt = got.NextDagRunAlt
	}
	raw.IsPaused = got.IsPaused
	return raw, nil
}

func (c *Client) getDAG(ctx context.Context, dep Deployment, dagID string) (*dagAPI, error) {
	u := c.apiBase(dep) + "/dags/" + url.PathEscape(dagID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	c.auth(req)
	resp, err := c.http().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("get dag %s: %s %s", dagID, resp.Status, shortErrBody(body))
	}
	var raw dagAPI
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}
	return &raw, nil
}

func (c *Client) latestRun(ctx context.Context, dep Deployment, dagID, state string) (*dagRunAPI, error) {
	runs, err := c.listRuns(ctx, dep, dagID, state, 1, 1)
	if err != nil || len(runs) == 0 {
		return nil, err
	}
	return &runs[0], nil
}

func (c *Client) listRuns(ctx context.Context, dep Deployment, dagID, state string, pageSize, maxPages int) ([]dagRunAPI, error) {
	if pageSize <= 0 {
		pageSize = runPageSize
	}
	if maxPages <= 0 {
		maxPages = 1
	}
	var all []dagRunAPI
	for page := 0; page < maxPages; page++ {
		batch, err := c.fetchRunPage(ctx, dep, dagID, state, pageSize, page*pageSize)
		if err != nil {
			if len(all) > 0 {
				return all, nil
			}
			return nil, err
		}
		all = append(all, batch...)
		if len(batch) < pageSize {
			break
		}
	}
	return all, nil
}

func (c *Client) fetchRunPage(ctx context.Context, dep Deployment, dagID, state string, limit, offset int) ([]dagRunAPI, error) {
	q := url.Values{}
	q.Set("limit", fmt.Sprintf("%d", limit))
	q.Set("offset", fmt.Sprintf("%d", offset))
	q.Set("order_by", "-start_date")
	if state != "" {
		q.Set("state", state)
	}
	u := c.apiBase(dep) + "/dags/" + url.PathEscape(dagID) + "/dagRuns?" + q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	c.auth(req)
	resp, err := c.http().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("dagRuns %s: %s %s", dagID, resp.Status, shortErrBody(body))
	}
	var payload struct {
		DagRuns []dagRunAPI `json:"dag_runs"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, err
	}
	return payload.DagRuns, nil
}

func runFacts(runs []dagRunAPI) []pipeline.RunFact {
	out := make([]pipeline.RunFact, 0, len(runs))
	for _, r := range runs {
		t := parseTS(r.StartDate)
		if t == nil {
			continue
		}
		out = append(out, pipeline.RunFact{
			Started: *t,
			Success: strings.EqualFold(r.State, "success"),
		})
	}
	return out
}

func (c *Client) auth(req *http.Request) {
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	req.Header.Set("Accept", "application/json")
}

func normalize(dep Deployment, dp domain.DataProduct, raw dagAPI, run, lastSuccess *dagRunAPI) domain.DAG {
	d := domain.DAG{
		DataProductID:   dp.ID,
		DataProductName: dp.Name,
		DeploymentName:  dep.Name,
		DeploymentID:    dep.ID,
		DAGID:           raw.DAGID,
		IsPaused:        raw.IsPaused,
		AstroURL:        GridURL(dep, raw.DAGID),
	}
	if strings.Contains(d.AstroURL, unknownDeploymentID) {
		d.AstroURL = ""
	}
	parseTags(&d, parseTagNames(raw.Tags))
	inferCustomFromID(&d)
	applySLADefaults(&d, raw.scheduleText())
	next := raw.nextRun()
	if d.IsPrimary || d.SLAMinutes != nil || d.IntervalMins != nil || next != "" {
		d.SilentMonitored = true
	}
	if run != nil {
		d.RunID = run.DAGRunID
		d.Status = strings.ToUpper(run.State)
		d.ErrorMessage = run.Note
		d.TriggerType = pipeline.TriggerFromRunID(run.DAGRunID, run.RunType)
		if t := parseTS(run.StartDate); t != nil {
			d.StartedAt = t
		}
		if t := parseTS(run.EndDate); t != nil {
			d.CompletedAt = t
			if d.StartedAt != nil {
				d.DurationSeconds = t.Sub(*d.StartedAt).Seconds()
			}
		}
	}
	if lastSuccess != nil {
		if t := parseTS(lastSuccess.EndDate); t != nil {
			d.LastSuccessAt = t
		} else if t := parseTS(lastSuccess.StartDate); t != nil {
			d.LastSuccessAt = t
		}
	}
	if t := parseTS(next); t != nil {
		d.NextExpectedAt = t
	}
	if d.NextExpectedAt == nil && d.LastSuccessAt != nil && d.IntervalMins != nil {
		n := d.LastSuccessAt.Add(time.Duration(*d.IntervalMins) * time.Minute)
		d.NextExpectedAt = &n
	}
	d.PipelineType = "DAG"
	if strings.TrimSpace(d.FrequencyDisplay) == "" {
		d.FrequencyDisplay = pipeline.FrequencyDisplay(d.IntervalMins)
	}
	return d
}

func (d dagAPI) nextRun() string {
	if strings.TrimSpace(d.NextDagRun) != "" {
		return d.NextDagRun
	}
	return d.NextDagRunAlt
}

func parseTags(d *domain.DAG, tags []string) {
	for _, name := range tags {
		name = strings.ToLower(strings.TrimSpace(name))
		switch {
		case name == "is_primary_dag":
			d.IsPrimary = true
		case name == "is_custom_dag_tag" || name == "is_custom_dag":
			d.IsCustom = true
		case strings.HasPrefix(name, "sla:interval_mins:"):
			v := numSuffix(name)
			if v > 0 {
				d.IntervalMins = &v
			}
		case strings.HasPrefix(name, "sla:threshold_mins:"):
			v := numSuffix(name)
			if v > 0 {
				d.SLAMinutes = &v
			}
		case strings.HasPrefix(name, "sla:frequency_display:"):
			label := strings.TrimSpace(strings.TrimPrefix(name, "sla:frequency_display:"))
			if label != "" {
				d.FrequencyDisplay = "Every " + label
			}
		}
	}
}

func inferCustomFromID(d *domain.DAG) {
	if d.IsCustom {
		return
	}
	for _, tok := range dagIDTokens(d.DAGID) {
		if tok == "custom" {
			d.IsCustom = true
			return
		}
	}
}

func numSuffix(tag string) float64 {
	parts := strings.Split(tag, ":")
	if len(parts) == 0 {
		return 0
	}
	var v float64
	fmt.Sscanf(parts[len(parts)-1], "%f", &v)
	return v
}

func parseTS(s string) *time.Time {
	if s == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339, strings.ReplaceAll(s, "Z", "+00:00"))
	if err != nil {
		t, err = time.Parse("2006-01-02T15:04:05.999999-07:00", s)
		if err != nil {
			return nil
		}
	}
	u := t.UTC()
	return &u
}
