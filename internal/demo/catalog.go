package demo

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/inorbit/inorbit/internal/config"
	"github.com/inorbit/inorbit/internal/domain"
)

// Resolve finds the demo catalog JSON. Prefer an explicit path, then
// INORBIT_CONFIG / configs/demo.json, then the copy next to this module.
func Resolve(explicit string) (string, error) {
	if p := strings.TrimSpace(explicit); p != "" {
		if _, err := os.Stat(p); err != nil {
			return "", fmt.Errorf("demo catalog %s: %w", p, err)
		}
		return p, nil
	}
	for _, p := range catalogCandidates() {
		if st, err := os.Stat(p); err == nil && !st.IsDir() {
			return p, nil
		}
	}
	return "", fmt.Errorf("demo catalog not found; pass -config configs/demo.json")
}

func catalogCandidates() []string {
	out := []string{"configs/demo.json"}
	if wd, err := os.Getwd(); err == nil {
		dir := wd
		for i := 0; i < 10; i++ {
			out = append(out, filepath.Join(dir, "configs", "demo.json"))
			parent := filepath.Dir(dir)
			if parent == dir {
				break
			}
			dir = parent
		}
	}
	if _, file, _, ok := runtime.Caller(0); ok {
		out = append(out, filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "configs", "demo.json")))
	}
	return out
}

// Load reads configs/demo.json (or INORBIT_CONFIG) relative to now.
func Load(now time.Time) ([]domain.DataProduct, []domain.DAG, []domain.Check, error) {
	path, err := Resolve("")
	if err != nil {
		return nil, nil, nil, err
	}
	return LoadFile(path, now)
}

// LoadFile materializes the demo snapshot from a config file.
func LoadFile(path string, now time.Time) ([]domain.DataProduct, []domain.DAG, []domain.Check, error) {
	cfg, err := config.Load(path)
	if err != nil {
		return nil, nil, nil, err
	}
	products := cfg.Products()
	if len(products) == 0 {
		return nil, nil, nil, fmt.Errorf("%s: no data_products", path)
	}
	byID := map[string]domain.DataProduct{}
	for _, p := range products {
		byID[p.ID] = p
	}
	now = now.UTC()
	dags := make([]domain.DAG, 0, len(cfg.Demo.DAGs))
	for i, raw := range cfg.Demo.DAGs {
		d, err := materializeDAG(raw, byID, now)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("%s: demo.dags[%d]: %w", path, i, err)
		}
		dags = append(dags, d)
	}
	checks := make([]domain.Check, 0, len(cfg.Demo.Checks))
	for i, raw := range cfg.Demo.Checks {
		c, err := materializeCheck(raw, byID, now)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("%s: demo.checks[%d]: %w", path, i, err)
		}
		checks = append(checks, c)
	}
	return products, dags, checks, nil
}

func materializeDAG(raw config.DemoDAG, products map[string]domain.DataProduct, now time.Time) (domain.DAG, error) {
	id := strings.ToLower(strings.TrimSpace(raw.DataProductID))
	p, ok := products[id]
	if !ok {
		return domain.DAG{}, fmt.Errorf("unknown data_product_id %q", raw.DataProductID)
	}
	completed, err := parseAgo(now, raw.CompletedAgo)
	if err != nil {
		return domain.DAG{}, err
	}
	d := domain.DAG{
		DataProductID:   p.ID,
		DataProductName: p.Name,
		DeploymentName:  raw.DeploymentName,
		DeploymentID:    raw.DeploymentID,
		DAGID:           raw.DAGID,
		RunID:           raw.RunID,
		Status:          raw.Status,
		CompletedAt:     completed,
		DurationSeconds: raw.DurationSeconds,
		IsPrimary:       raw.IsPrimary,
		IsPaused:        raw.IsPaused,
		IsCustom:        raw.IsCustom,
		SilentMonitored: raw.SilentMonitored,
		AstroURL:        raw.AstroURL,
		TriggerType:     raw.TriggerType,
		PipelineType:    raw.PipelineType,
		Runs7d:          raw.Runs7d,
		Reliability7d:   raw.Reliability7d,
		Runs30d:         raw.Runs30d,
		Reliability30d:  raw.Reliability30d,
		Runs90d:         raw.Runs90d,
		Reliability90d:  raw.Reliability90d,
	}
	if raw.IntervalMins > 0 {
		v := raw.IntervalMins
		d.IntervalMins = &v
	}
	if raw.SLAMinutes > 0 {
		v := raw.SLAMinutes
		d.SLAMinutes = &v
	}
	if completed != nil {
		if strings.EqualFold(raw.Status, "SUCCESS") {
			d.LastSuccessAt = completed
		}
		if raw.DurationSeconds > 0 {
			start := completed.Add(-time.Duration(raw.DurationSeconds * float64(time.Second)))
			d.StartedAt = &start
		}
		if d.IntervalMins != nil {
			next := completed.Add(time.Duration(*d.IntervalMins) * time.Minute)
			d.NextExpectedAt = &next
		}
	}
	return d, nil
}

func materializeCheck(raw config.DemoCheck, products map[string]domain.DataProduct, now time.Time) (domain.Check, error) {
	id := strings.ToLower(strings.TrimSpace(raw.DataProductID))
	p, ok := products[id]
	if !ok {
		return domain.Check{}, fmt.Errorf("unknown data_product_id %q", raw.DataProductID)
	}
	executed, err := parseAgo(now, raw.ExecutedAgo)
	if err != nil {
		return domain.Check{}, err
	}
	failed, err := parseAgo(now, raw.FirstFailedAgo)
	if err != nil {
		return domain.Check{}, err
	}
	c := domain.Check{
		ID:              raw.ID,
		DataProductID:   p.ID,
		DataProductName: p.Name,
		Name:            raw.Name,
		Dimension:       domain.Dimension(strings.ToUpper(raw.Dimension)),
		Severity:        domain.Severity(strings.ToUpper(raw.Severity)),
		Status:          domain.CheckStatus(strings.ToUpper(raw.Status)),
		SourceType:      domain.SourceType(strings.ToUpper(raw.SourceType)),
		SourceTable:     raw.SourceTable,
		IsCDE:           raw.IsCDE,
		Element:         raw.Element,
		FirstFailedAt:   failed,
	}
	if executed != nil {
		c.ExecutedAt = *executed
	}
	return c, nil
}

func parseAgo(now time.Time, spec string) (*time.Time, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return nil, nil
	}
	d, err := time.ParseDuration(spec)
	if err != nil {
		return nil, fmt.Errorf("duration %q: %w", spec, err)
	}
	t := now.Add(-d)
	return &t, nil
}
