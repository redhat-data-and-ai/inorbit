package quality

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/pem"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/inorbit/inorbit/internal/domain"
	sf "github.com/snowflakedb/gosnowflake"
)

var identRe = regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`)

type Snowflake struct {
	DB   *sql.DB
	sess *sql.Conn
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
		// keychain so a later process can skip the browser. This process also
		// pins one sql.Conn so polls do not open a new session (and a new tab).
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
	return &Snowflake{DB: db, sess: sess}, nil
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

// LatestChecks loads the newest ValidX run and newest dbt/Elementary invocation per product.
func (s *Snowflake) LatestChecks(ctx context.Context, products []domain.DataProduct) (checks []domain.Check, productIDs []string, err error) {
	if s == nil || s.sess == nil {
		return nil, nil, fmt.Errorf("snowflake client is nil")
	}
	var errs []string
	for _, p := range products {
		got, perr := s.productChecks(ctx, p)
		if perr != nil {
			errs = append(errs, p.ID+": "+perr.Error())
			if len(got) == 0 {
				continue
			}
		}
		productIDs = append(productIDs, p.ID)
		checks = append(checks, got...)
	}
	if len(productIDs) == 0 && len(errs) > 0 {
		return nil, nil, fmt.Errorf("quality ingest: %s", strings.Join(errs, "; "))
	}
	if len(errs) > 0 {
		return checks, productIDs, fmt.Errorf("quality ingest partial: %s", strings.Join(errs, "; "))
	}
	return checks, productIDs, nil
}

func (s *Snowflake) productChecks(ctx context.Context, p domain.DataProduct) ([]domain.Check, error) {
	var out []domain.Check
	var errs []string

	if p.ValidX.Enabled {
		rel, err := qualified(p.ValidX)
		if err != nil {
			errs = append(errs, "validx: "+err.Error())
		} else {
			vxSQL := `
with latest as (
  select run_id
  from ` + rel + `
  qualify row_number() over (order by run_time desc nulls last) = 1
)
select vr.*
from ` + rel + ` vr
inner join latest on latest.run_id = vr.run_id`
			vx, err := s.query(ctx, vxSQL)
			if err != nil {
				if !isMissingObject(err) {
					errs = append(errs, "validx: "+err.Error())
				}
			} else {
				for _, rec := range vx {
					stripSensitive(rec)
					if c, ok := ValidXCheck(p, rec); ok {
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
		} else {
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
			dbt, err := s.query(ctx, dbtSQL)
			if err != nil {
				if !isMissingObject(err) {
					fallback, ferr := s.query(ctx, "select * from "+rel+" limit 4000")
					if ferr != nil {
						if !isMissingObject(ferr) {
							errs = append(errs, "dbt: "+err.Error())
						}
					} else {
						fallback = KeepLatestRun(fallback, []string{"DETECTED_AT", "GENERATED_AT", "CREATED_AT"}, []string{"INVOCATION_ID", "TEST_EXECUTION_ID"})
						for _, rec := range fallback {
							stripSensitive(rec)
							if c, ok := DBTCheck(p, rec); ok {
								out = append(out, c)
							}
						}
					}
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
		return nil, fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	if len(errs) > 0 {
		return out, fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	return out, nil
}

func stripSensitive(rec map[string]any) {
	for _, k := range []string{"QUERY", "VALIDATION_QUERY", "EXPECTED_OUTPUT", "ACTUAL_OUTPUT", "EXPECTED", "ACTUAL"} {
		delete(rec, k)
	}
}

func isMissingObject(err error) bool {
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "does not exist") || strings.Contains(s, "object does not exist")
}

func (s *Snowflake) query(ctx context.Context, q string) ([]map[string]any, error) {
	if s == nil || s.sess == nil {
		return nil, fmt.Errorf("snowflake session is nil")
	}
	// Keep the process context for shutdown, but do not inherit a poll timeout
	// that cancels after success — that closes the SSO session.
	rows, err := s.sess.QueryContext(withoutPollDeadline(ctx), q)
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
