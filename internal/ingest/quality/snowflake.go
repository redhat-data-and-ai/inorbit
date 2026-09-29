package quality

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/pem"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/inorbit/inorbit/internal/domain"
	sf "github.com/snowflakedb/gosnowflake"
)

var identRe = regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`)

var quietDriverOnce sync.Once

// qualityFetchConc is how many warehouse products we query at once after SSO.
// dbt tests follow the pipeline, so wall time matters more than poll frequency.
const qualityFetchConc = 8

type Snowflake struct {
	DB      *sql.DB
	sess    *sql.Conn
	mu      sync.Mutex
	missing map[string]bool
}

type ConnConfig struct {
	Account       string
	User          string
	Role          string
	Warehouse     string
	Database      string
	Authenticator string
}

func OpenFromEnv() (*Snowflake, error) {
	return Open(ConnConfig{})
}

func Open(c ConnConfig) (*Snowflake, error) {
	quietMissingObjectDriverLogs()
	account := pick(c.Account, "SNOWFLAKE_ACCOUNT")
	user := pick(c.User, "SNOWFLAKE_USER")
	if account == "" || user == "" {
		return nil, fmt.Errorf("set snowflake.account in configs/live.json (or SNOWFLAKE_ACCOUNT) and SNOWFLAKE_USER")
	}
	keepAlive := "true"
	cfg := &sf.Config{
		Account:   account,
		User:      user,
		Role:      pick(c.Role, "SNOWFLAKE_ROLE"),
		Warehouse: pick(c.Warehouse, "SNOWFLAKE_WAREHOUSE"),
		Database:  pick(c.Database, "SNOWFLAKE_DATABASE"),
		Params:    map[string]*string{"client_session_keep_alive": &keepAlive},
		// Same as dbt/snowflake-connector: cache the SSO ID token in the OS
		// keychain so extra pool connections reuse it (no extra browser tabs).
		ClientStoreTemporaryCredential: sf.ConfigBoolTrue,
		ClientRequestMfaToken:          sf.ConfigBoolTrue,
		ExternalBrowserTimeout:         5 * time.Minute,
		LoginTimeout:                   5 * time.Minute,
	}
	pemEnv := strings.TrimSpace(os.Getenv("SNOWFLAKE_PRIVATE_KEY"))
	switch {
	case strings.TrimSpace(os.Getenv("SNOWFLAKE_PRIVATE_KEY_PATH")) != "" || pemEnv != "":
		var key *rsa.PrivateKey
		var err error
		if path := strings.TrimSpace(os.Getenv("SNOWFLAKE_PRIVATE_KEY_PATH")); path != "" {
			key, err = loadPEMFile(path)
		} else if strings.Contains(pemEnv, "BEGIN") {
			key, err = parsePEM([]byte(pemEnv))
		} else {
			key, err = loadPEMFile(pemEnv)
		}
		if err != nil {
			return nil, err
		}
		cfg.Authenticator = sf.AuthTypeJwt
		cfg.PrivateKey = key
	case strings.TrimSpace(os.Getenv("SNOWFLAKE_PASSWORD")) != "":
		cfg.Password = os.Getenv("SNOWFLAKE_PASSWORD")
	default:
		auth := strings.ToLower(strings.TrimSpace(c.Authenticator))
		if auth == "" {
			auth = strings.ToLower(strings.TrimSpace(os.Getenv("SNOWFLAKE_AUTHENTICATOR")))
		}
		if auth == "" || auth == "externalbrowser" {
			cfg.Authenticator = sf.AuthTypeExternalBrowser
		} else {
			return nil, fmt.Errorf("unsupported SNOWFLAKE_AUTHENTICATOR %s (use password, key, or externalbrowser)", auth)
		}
	}
	db := sql.OpenDB(sf.NewConnector(sf.SnowflakeDriver{}, *cfg))
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	db.SetConnMaxIdleTime(0)
	// Authenticate once here. Do not wrap this in a short timeout: the driver
	// waits on the browser (up to ExternalBrowserTimeout). Cancelling Ping /
	// Query after a poll marks the session bad and the next poll opens SSO again.
	sess, err := db.Conn(context.Background())
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := sess.PingContext(context.Background()); err != nil {
		_ = sess.Close()
		_ = db.Close()
		return nil, err
	}
	// Keep the first Conn so the SSO session stays warm. Raise the pool so
	// LatestChecks can run qualityFetchConc queries on other connections that
	// reuse the cached ID token.
	db.SetMaxOpenConns(qualityFetchConc + 1)
	db.SetMaxIdleConns(qualityFetchConc + 1)
	return &Snowflake{DB: db, sess: sess, missing: map[string]bool{}}, nil
}

func loadPEMFile(path string) (*rsa.PrivateKey, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return parsePEM(raw)
}

func parsePEM(raw []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, fmt.Errorf("no PEM block in snowflake private key")
	}
	pass := []byte(os.Getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"))
	var der []byte
	if x509.IsEncryptedPEMBlock(block) { //nolint:staticcheck
		if len(pass) == 0 {
			return nil, fmt.Errorf("encrypted key; set SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
		}
		decoded, err := x509.DecryptPEMBlock(block, pass) //nolint:staticcheck
		if err != nil {
			return nil, err
		}
		der = decoded
	} else {
		der = block.Bytes
	}
	if k, err := x509.ParsePKCS8PrivateKey(der); err == nil {
		rk, ok := k.(*rsa.PrivateKey)
		if !ok {
			return nil, fmt.Errorf("not an RSA private key")
		}
		return rk, nil
	}
	return x509.ParsePKCS1PrivateKey(der)
}

func pick(v, envKey string) string {
	if strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	return strings.TrimSpace(os.Getenv(envKey))
}

func quoteIdent(s string) (string, error) {
	s = strings.ToUpper(strings.TrimSpace(s))
	if !identRe.MatchString(s) {
		return "", fmt.Errorf("invalid identifier %q", s)
	}
	return s, nil
}

func qualified(t domain.QualityTable) (string, error) {
	db, err := quoteIdent(t.Database)
	if err != nil {
		return "", err
	}
	sch, err := quoteIdent(t.Schema)
	if err != nil {
		return "", err
	}
	tbl, err := quoteIdent(t.Table)
	if err != nil {
		return "", err
	}
	return db + "." + sch + "." + tbl, nil
}

func (s *Snowflake) Close() error {
	if s == nil {
		return nil
	}
	var err error
	if s.sess != nil {
		err = s.sess.Close()
		s.sess = nil
	}
	if s.DB != nil {
		if e := s.DB.Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
}

func (s *Snowflake) ready() bool {
	return s != nil && s.DB != nil
}

// LatestChecks loads the newest validation RUN_ID and newest dbt/Elementary invocation per product.
// Missing warehouse objects are skipped, not treated as ingest errors.
func (s *Snowflake) LatestChecks(ctx context.Context, products []domain.DataProduct) (checks []domain.Check, sources map[string]domain.QualitySources, err error) {
	if !s.ready() {
		return nil, nil, fmt.Errorf("snowflake client is nil")
	}
	sources = make(map[string]domain.QualitySources, len(products))
	if len(products) == 0 {
		return nil, sources, nil
	}
	type result struct {
		id     string
		checks []domain.Check
		src    domain.QualitySources
		err    error
	}
	ch := make(chan result, len(products))
	sem := make(chan struct{}, qualityFetchConc)
	var wg sync.WaitGroup
	for _, p := range products {
		p := p
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case <-ctx.Done():
				ch <- result{id: p.ID, err: ctx.Err()}
				return
			case sem <- struct{}{}:
			}
			defer func() { <-sem }()
			got, src, perr := s.productChecks(ctx, p)
			ch <- result{id: p.ID, checks: got, src: src, err: perr}
		}()
	}
	go func() {
		wg.Wait()
		close(ch)
	}()
	var errs []string
	for res := range ch {
		sources[res.id] = res.src
		if res.err != nil {
			errs = append(errs, res.id+": "+res.err.Error())
		}
		checks = append(checks, res.checks...)
	}
	if len(errs) == len(products) && len(checks) == 0 && len(products) > 0 {
		return checks, sources, fmt.Errorf("quality ingest: %s", strings.Join(errs, "; "))
	}
	if len(errs) > 0 {
		return checks, sources, fmt.Errorf("quality ingest partial: %s", strings.Join(errs, "; "))
	}
	return checks, sources, nil
}

func disabledSource() domain.QualitySource {
	return domain.QualitySource{Status: domain.QualityDisabled}
}

func (s *Snowflake) productChecks(ctx context.Context, p domain.DataProduct) ([]domain.Check, domain.QualitySources, error) {
	var out []domain.Check
	var errs []string
	src := domain.QualitySources{
		Validation: disabledSource(),
		DBT:        disabledSource(),
	}

	if p.Validation.Enabled {
		rel, err := qualified(p.Validation)
		if err != nil {
			errs = append(errs, "validation: "+err.Error())
			src.Validation = domain.QualitySource{Status: domain.QualityMissing}
		} else if s.knownMissing(rel) {
			src.Validation = domain.QualitySource{Status: domain.QualityMissing, Relation: rel}
		} else {
			src.Validation = domain.QualitySource{Status: domain.QualityOK, Relation: rel}
			vxSQL := `
with staged as (
  select * from ` + rel + `
),
latest as (
  select run_id
  from staged
  qualify row_number() over (order by run_time desc nulls last) = 1
)
select s.*
from staged s
inner join latest l on s.run_id = l.run_id`
			vx, err := s.latestRows(ctx, rel, vxSQL, func(rows []map[string]any) []map[string]any {
				return KeepLatestRun(rows, []string{"RUN_TIME", "CREATED_AT", "UPDATED_AT", "EXECUTED_AT"}, []string{"RUN_ID"})
			})
			if err != nil {
				if isMissingObject(err) {
					src.Validation.Status = domain.QualityMissing
				} else {
					errs = append(errs, "validation: "+err.Error())
				}
			} else {
				for _, rec := range vx {
					stripSensitive(rec)
					if c, ok := ValidationCheck(p, rec); ok {
						out = append(out, c)
					}
				}
			}
		}
	}

	if p.DBTLogs.Enabled {
		rel, err := qualified(p.DBTLogs)
		if err != nil {
			errs = append(errs, "dbt: "+err.Error())
			src.DBT = domain.QualitySource{Status: domain.QualityMissing}
		} else if s.knownMissing(rel) {
			src.DBT = domain.QualitySource{Status: domain.QualityMissing, Relation: rel}
		} else {
			src.DBT = domain.QualitySource{Status: domain.QualityOK, Relation: rel}
			dbtSQL := `
with staged as (
  select * from ` + rel + `
),
latest as (
  select coalesce(invocation_id, test_execution_id) as invocation_key
  from staged
  qualify row_number() over (order by detected_at desc nulls last) = 1
)
select s.*
from staged s
inner join latest l on coalesce(s.invocation_id, s.test_execution_id) = l.invocation_key`
			dbt, err := s.latestRows(ctx, rel, dbtSQL, func(rows []map[string]any) []map[string]any {
				return KeepLatestRun(rows, []string{"DETECTED_AT", "GENERATED_AT", "CREATED_AT"}, []string{"INVOCATION_ID", "TEST_EXECUTION_ID"})
			})
			if err != nil {
				if isMissingObject(err) {
					src.DBT.Status = domain.QualityMissing
				} else {
					errs = append(errs, "dbt: "+err.Error())
				}
			} else {
				for _, rec := range dbt {
					stripSensitive(rec)
					if c, ok := DBTCheck(p, rec); ok {
						out = append(out, c)
					}
				}
			}
		}
	}

	if len(out) == 0 && len(errs) > 0 {
		return nil, src, fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	if len(errs) > 0 {
		return out, src, fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	return out, src, nil
}

func stripSensitive(rec map[string]any) {
	for _, k := range []string{"QUERY", "VALIDATION_QUERY", "EXPECTED_OUTPUT", "ACTUAL_OUTPUT", "EXPECTED", "ACTUAL"} {
		delete(rec, k)
	}
}

func isMissingObject(err error) bool {
	if err == nil {
		return false
	}
	return isMissingObjectLog(err.Error())
}

func isBadIdentifier(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "000904") || strings.Contains(s, "invalid identifier")
}

func isSchemaMismatch(err error) bool {
	if isBadIdentifier(err) {
		return true
	}
	if err == nil || isMissingObject(err) {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "sql compilation error")
}

func isMissingObjectLog(s string) bool {
	s = strings.ToLower(s)
	return strings.Contains(s, "002003") ||
		strings.Contains(s, "does not exist") ||
		strings.Contains(s, "object does not exist")
}

func isNoisyDriverLog(s string) bool {
	if isMissingObjectLog(s) {
		return true
	}
	l := strings.ToLower(s)
	return strings.Contains(l, "invalid identifier")
}

func (s *Snowflake) knownMissing(rel string) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.missing[rel]
}

func (s *Snowflake) markMissing(rel string) {
	if s == nil || rel == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.missing == nil {
		s.missing = map[string]bool{}
	}
	s.missing[rel] = true
}

// quietMissingObjectDriverLogs drops gosnowflake ERRO lines for 002003 (schema/table
// missing) and 000904 invalid identifier (tables without RUN_ID). InOrbit
// already treats those as skipped sources or a fallback scan; the driver logs first.
func quietMissingObjectDriverLogs() {
	quietDriverOnce.Do(func() {
		sf.GetLogger().SetOutput(missingObjectLogWriter{w: os.Stderr})
	})
}

type missingObjectLogWriter struct {
	w io.Writer
}

func (m missingObjectLogWriter) Write(p []byte) (int, error) {
	if isNoisyDriverLog(string(p)) {
		return len(p), nil
	}
	if m.w == nil {
		return os.Stderr.Write(p)
	}
	return m.w.Write(p)
}

func (s *Snowflake) latestRows(ctx context.Context, rel, preferred string, keep func([]map[string]any) []map[string]any) ([]map[string]any, error) {
	rows, err := s.query(ctx, preferred)
	if err == nil {
		return rows, nil
	}
	if isMissingObject(err) {
		s.markMissing(rel)
		return nil, err
	}
	if !isSchemaMismatch(err) {
		return nil, err
	}
	raw, ferr := s.query(ctx, "select * from "+rel+" limit 4000")
	if ferr != nil {
		if isMissingObject(ferr) {
			s.markMissing(rel)
			return nil, ferr
		}
		return nil, err
	}
	if keep == nil {
		return raw, nil
	}
	return keep(raw), nil
}

func (s *Snowflake) query(ctx context.Context, q string) ([]map[string]any, error) {
	if !s.ready() {
		return nil, fmt.Errorf("snowflake client is nil")
	}
	// Keep the process context for shutdown, but do not inherit a poll timeout
	// that cancels after success — that closes the SSO session.
	rows, err := s.DB.QueryContext(withoutPollDeadline(ctx), q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanMaps(rows)
}

func withoutPollDeadline(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return context.WithoutCancel(ctx)
}

func scanMaps(rows *sql.Rows) ([]map[string]any, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var out []map[string]any
	for rows.Next() {
		raw := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range raw {
			ptrs[i] = &raw[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		rec := map[string]any{}
		for i, c := range cols {
			rec[strings.ToUpper(c)] = unwrap(raw[i])
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

func unwrap(v any) any {
	switch t := v.(type) {
	case []byte:
		return string(t)
	default:
		return t
	}
}
