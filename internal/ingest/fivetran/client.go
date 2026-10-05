package fivetran

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
)

const (
	defaultAPIBase      = "https://api.fivetran.com/v1"
	defaultDashboardURL = "https://fivetran.com/dashboard/connections"
	pageLimit           = "1000"
	groupFetchConc      = 4
)

type Client struct {
	HTTP         *http.Client
	APIKey       string
	APISecret    string
	BaseURL      string
	DashboardURL string
	GroupIDs     []string
	Log          *log.Logger
}

type groupAPI struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type statusNote struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type connectionStatusAPI struct {
	SetupState       string       `json:"setup_state"`
	SyncState        string       `json:"sync_state"`
	UpdateState      string       `json:"update_state"`
	IsHistoricalSync bool         `json:"is_historical_sync"`
	Tasks            []statusNote `json:"tasks"`
	Warnings         []statusNote `json:"warnings"`
}

type connectionAPI struct {
	ID            string              `json:"id"`
	GroupID       string              `json:"group_id"`
	Service       string              `json:"service"`
	Schema        string              `json:"schema"`
	Paused        bool                `json:"paused"`
	SucceededAt   string              `json:"succeeded_at"`
	FailedAt      string              `json:"failed_at"`
	SyncFrequency float64             `json:"sync_frequency"`
	ScheduleType  string              `json:"schedule_type"`
	Status        connectionStatusAPI `json:"status"`
}

type listPage struct {
	Code string `json:"code"`
	Data struct {
		Items      json.RawMessage `json:"items"`
		NextCursor string          `json:"next_cursor"`
	} `json:"data"`
}

func (c *Client) http() *http.Client {
	if c.HTTP == nil {
		t := http.DefaultTransport.(*http.Transport).Clone()
		t.MaxIdleConns = 32
		t.MaxIdleConnsPerHost = 8
		t.IdleConnTimeout = 90 * time.Second
		t.ResponseHeaderTimeout = 20 * time.Second
		c.HTTP = &http.Client{Timeout: 30 * time.Second, Transport: t}
	}
	return c.HTTP
}

func (c *Client) base() string {
	u := strings.TrimRight(strings.TrimSpace(c.BaseURL), "/")
	if u == "" {
		return defaultAPIBase
	}
	return u
}

func (c *Client) dashboard() string {
	u := strings.TrimRight(strings.TrimSpace(c.DashboardURL), "/")
	if u == "" {
		return defaultDashboardURL
	}
	return u
}

func (c *Client) logf(format string, args ...any) {
	if c.Log != nil {
		c.Log.Printf("fivetran "+format, args...)
	}
}

// FetchForProducts lists Fivetran connections and keeps those that match
// source-aligned catalog products (explicit connector id, schema, or service).
func (c *Client) FetchForProducts(ctx context.Context, products []domain.DataProduct, extra map[string]string) ([]domain.Connector, error) {
	if strings.TrimSpace(c.APIKey) == "" || strings.TrimSpace(c.APISecret) == "" {
		return nil, nil
	}
	if len(sourceAligned(products)) == 0 && len(extra) == 0 {
		return nil, nil
	}
	groups, err := c.listGroups(ctx)
	if err != nil {
		return nil, err
	}
	groups = c.filterGroups(groups)
	if len(groups) == 0 {
		return nil, nil
	}

	type result struct {
		conns []domain.Connector
		err   error
	}
	ch := make(chan result, len(groups))
	sem := make(chan struct{}, groupFetchConc)
	var wg sync.WaitGroup
	now := time.Now().UTC()
	for _, g := range groups {
		wg.Add(1)
		go func(g groupAPI) {
			defer wg.Done()
			select {
			case <-ctx.Done():
				ch <- result{err: ctx.Err()}
				return
			case sem <- struct{}{}:
			}
			defer func() { <-sem }()
			conns, err := c.listConnections(ctx, g.ID)
			if err != nil {
				c.logf("%s: %v", g.Name, err)
				ch <- result{err: err}
				return
			}
			var matched []domain.Connector
			for _, conn := range conns {
				p, ok := Match(conn.ID, conn.Schema, conn.Service, products, extra)
				if !ok {
					continue
				}
				matched = append(matched, toConnector(p, g, conn, c.dashboard(), now))
			}
			ch <- result{conns: matched}
		}(g)
	}
	go func() {
		wg.Wait()
		close(ch)
	}()

	var out []domain.Connector
	var last error
	for res := range ch {
		if res.err != nil {
			last = res.err
			continue
		}
		out = append(out, res.conns...)
	}
	if len(out) == 0 {
		return out, last
	}
	return out, nil
}

func (c *Client) filterGroups(groups []groupAPI) []groupAPI {
	if len(c.GroupIDs) == 0 {
		return groups
	}
	want := map[string]struct{}{}
	for _, id := range c.GroupIDs {
		id = strings.TrimSpace(id)
		if id != "" {
			want[id] = struct{}{}
			want[strings.ToLower(id)] = struct{}{}
		}
	}
	if len(want) == 0 {
		return groups
	}
	var out []groupAPI
	for _, g := range groups {
		if _, ok := want[g.ID]; ok {
			out = append(out, g)
			continue
		}
		if _, ok := want[strings.ToLower(g.Name)]; ok {
			out = append(out, g)
		}
	}
	return out
}

func (c *Client) listGroups(ctx context.Context) ([]groupAPI, error) {
	var out []groupAPI
	err := c.paged(ctx, "/groups", func(raw json.RawMessage) error {
		var items []groupAPI
		if err := json.Unmarshal(raw, &items); err != nil {
			return err
		}
		out = append(out, items...)
		return nil
	})
	return out, err
}

func (c *Client) listConnections(ctx context.Context, groupID string) ([]connectionAPI, error) {
	conns, err := c.listConnectionsAt(ctx, "/groups/"+url.PathEscape(groupID)+"/connections")
	if err == nil {
		return conns, nil
	}
	if !isNotFound(err) {
		return nil, err
	}
	return c.listConnectionsAt(ctx, "/groups/"+url.PathEscape(groupID)+"/connectors")
}

func (c *Client) listConnectionsAt(ctx context.Context, path string) ([]connectionAPI, error) {
	var out []connectionAPI
	err := c.paged(ctx, path, func(raw json.RawMessage) error {
		var items []connectionAPI
		if err := json.Unmarshal(raw, &items); err != nil {
			return err
		}
		out = append(out, items...)
		return nil
	})
	return out, err
}

func (c *Client) paged(ctx context.Context, path string, each func(json.RawMessage) error) error {
	cursor := ""
	for {
		q := url.Values{}
		q.Set("limit", pageLimit)
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		var page listPage
		if err := c.getJSON(ctx, path+"?"+q.Encode(), &page); err != nil {
			return err
		}
		if len(page.Data.Items) > 0 && string(page.Data.Items) != "null" {
			if err := each(page.Data.Items); err != nil {
				return err
			}
		}
		cursor = strings.TrimSpace(page.Data.NextCursor)
		if cursor == "" {
			return nil
		}
	}
}

func (c *Client) getJSON(ctx context.Context, path string, dest any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base()+path, nil)
	if err != nil {
		return err
	}
	req.SetBasicAuth(c.APIKey, c.APISecret)
	req.Header.Set("Accept", "application/json")
	resp, err := c.http().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusNotFound {
		return notFoundError{path: path}
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s: HTTP %d %s", path, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if err := json.Unmarshal(body, dest); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}

type notFoundError struct{ path string }

func (e notFoundError) Error() string { return e.path + ": HTTP 404" }

func isNotFound(err error) bool {
	_, ok := err.(notFoundError)
	return ok
}
