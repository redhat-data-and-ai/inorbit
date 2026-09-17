package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/inorbit/inorbit/internal/alert"
	"github.com/inorbit/inorbit/internal/api"
	"github.com/inorbit/inorbit/internal/config"
	"github.com/inorbit/inorbit/internal/demo"
	"github.com/inorbit/inorbit/internal/domain"
	"github.com/inorbit/inorbit/internal/engine"
	"github.com/inorbit/inorbit/internal/ingest/astro"
	"github.com/inorbit/inorbit/internal/ingest/quality"
	"github.com/inorbit/inorbit/internal/store"
	"github.com/inorbit/inorbit/internal/web"
)

type intervals struct {
	clock   time.Duration
	astro   time.Duration
	tags    time.Duration
	quality time.Duration
}

type liveAirflow struct {
	client   *astro.Client
	quality  *quality.Snowflake
	products []domain.DataProduct
	extra    map[string]string
}

func main() {
	loadDotEnv()
	addr := flag.String("addr", env("INORBIT_HTTP_ADDR", ":8080"), "HTTP listen address")
	mode := flag.String("mode", env("INORBIT_MODE", "demo"), "demo (catalog JSON) or live (Airflow API)")
	configPath := flag.String("config", env("INORBIT_CONFIG", ""), "JSON config path (demo catalog or live overlay)")
	clockSec := flag.Int("clock-seconds", 15, "SLA clock tick (no I/O)")
	astroSec := flag.Int("astro-run-seconds", 90, "Airflow latest-run poll")
	tagSec := flag.Int("astro-tag-seconds", 600, "Airflow DAG-tag poll")
	qualitySec := flag.Int("quality-seconds", 120, "quality / dbt watermark poll")
	flag.Parse()

	lg := log.New(os.Stdout, "inorbit ", log.LstdFlags)
	st := store.New()
	st.SetMeta(func(m *domain.PollMeta) {
		m.Mode = *mode
		m.ClockIntervalSeconds = *clockSec
		m.AstroRunPollSeconds = *astroSec
		m.AstroTagPollSeconds = *tagSec
		m.QualityPollSeconds = *qualitySec
	})
	eng := engine.New(st)
	alerts := alert.New(st, lg)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var live *liveAirflow
	now := time.Now().UTC()
	if *mode == "demo" {
		ids := loadDemo(lg, st, eng, now, *configPath)
		lg.Printf("demo catalog loaded: %d product(s) [%s]", len(ids), strings.Join(ids, ", "))
	} else {
		live = mustLive(lg, st, *configPath)
		now = time.Now().UTC()
		n, err := pollAirflow(ctx, lg, st, eng, live, now)
		if err != nil {
			lg.Printf("startup airflow ingest failed: %v", err)
		} else {
			lg.Printf("startup airflow ingest: %d dag(s)", n)
		}
		if qn, qerr := pollQuality(ctx, lg, st, eng, live, now); qerr != nil {
			lg.Printf("startup quality ingest: %v", qerr)
		} else if live != nil && live.quality != nil {
			lg.Printf("startup quality ingest: %d check(s)", qn)
		}
		sc := st.Meta().Scale
		lg.Printf("scale: %d product(s) snapshot JSON %dB → %d products ~%dB JSON / ~%dB RSS",
			sc.CurrentDataProducts, sc.SnapshotJSONBytes, sc.TargetDataProducts,
			sc.EstimatedSnapshotJSONBytesAtTarget, sc.EstimatedRSSBytesAtTarget)
	}

	srv := &api.Server{Store: st, Alerts: alerts}
	httpSrv := &http.Server{Addr: *addr, Handler: web.Mount(srv.Handler())}

	iv := intervals{
		clock:   time.Duration(*clockSec) * time.Second,
		astro:   time.Duration(*astroSec) * time.Second,
		tags:    time.Duration(*tagSec) * time.Second,
		quality: time.Duration(*qualitySec) * time.Second,
	}

	go runWorkers(ctx, lg, st, eng, alerts, iv, *mode, live)

	go func() {
		lg.Printf("listening on %s mode=%s clock=%s astro=%s tags=%s quality=%s",
			*addr, *mode, iv.clock, iv.astro, iv.tags, iv.quality)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			lg.Fatal(err)
		}
	}()

	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdown)
	if live != nil && live.quality != nil {
		_ = live.quality.Close()
	}
}

func mustLive(lg *log.Logger, st *store.Memory, configPath string) *liveAirflow {
	if configPath == "" {
		if _, err := os.Stat("configs/live.json"); err == nil {
			configPath = "configs/live.json"
		} else {
			configPath = "configs/live.example.json"
		}
	}
	cfg, err := config.Load(configPath)
	if err != nil {
		lg.Fatalf("load config %s: %v", configPath, err)
	}
	products := cfg.Products()
	if len(products) == 0 {
		lg.Fatalf("config %s: no data_products", configPath)
	}
	for _, p := range products {
		st.UpsertProduct(p)
	}
	deps := cfg.Deployments()
	token := cfg.Token()
	if token == "" {
		lg.Printf("warning: %s is empty; Airflow calls will 401", cfg.Astro.TokenEnv)
	}
	if len(deps) == 0 {
		lg.Printf("warning: no deployment URL; set ASTRO_ORG_URL + INORBIT_ASTRO_DEPLOYMENT_ID (or INORBIT_AIRFLOW_API_URL)")
	} else {
		lg.Printf("live config %s: %d data product(s), %d deployment(s)", configPath, len(products), len(deps))
		for _, d := range deps {
			if d.ID == "" {
				lg.Printf("warning: deployment %s has empty id; set INORBIT_ASTRO_DEPLOYMENT_ID", d.Name)
			}
			lg.Printf("airflow candidates %s: %v", d.Name, astro.AirflowAPIBases(d))
		}
	}
	sc := cfg.SnowflakeConn()
	sf, err := quality.Open(quality.ConnConfig{
		Account:       sc.Account,
		User:          sc.User,
		Role:          sc.Role,
		Warehouse:     sc.Warehouse,
		Database:      sc.Database,
		Authenticator: sc.Authenticator,
	})
	if err != nil {
		lg.Printf("warning: snowflake quality ingest disabled: %v", err)
	} else {
		lg.Printf("snowflake quality ingest enabled account=%s role=%s warehouse=%s (one SSO session, reused until exit)", sc.Account, sc.Role, sc.Warehouse)
		for _, p := range products {
			lg.Printf("quality sources %s: validx=%s dbt=%s", p.ID, describeTable(p.ValidX), describeTable(p.DBTLogs))
		}
	}
	var warns []string
	if token == "" {
		warns = append(warns, "ASTRO_TOKEN is empty, so Airflow returns 401 and every product stays at 0 DAGs with no health score. Put the token in .env (gitignored) or export ASTRO_TOKEN, then restart.")
	}
	if len(deps) == 0 {
		warns = append(warns, "No Airflow deployment URL. Set ASTRO_ORG_URL and astro.deployments[].id in the live config or .env.")
	}
	if sf == nil {
		warns = append(warns, "Snowflake quality ingest is off, so ValidX/Elementary scores are missing. Set SNOWFLAKE_USER (SSO) and restart.")
	}
	if len(warns) > 0 {
		st.SetMeta(func(m *domain.PollMeta) { m.Warnings = warns })
	}
	return &liveAirflow{
		client:   &astro.Client{Token: token, Deployments: deps, Log: lg},
		quality:  sf,
		products: products,
		extra:    cfg.DAGMap(),
	}
}

func pollAirflow(ctx context.Context, lg *log.Logger, st *store.Memory, eng *engine.Engine, live *liveAirflow, now time.Time) (int, error) {
	if live == nil || live.client == nil {
		return 0, nil
	}
	dags, err := live.client.FetchForProducts(ctx, live.products, live.extra)
	if err != nil {
		return 0, err
	}
	astro.Apply(st, eng, dags, now)
	st.TouchAstroRuns(time.Now().UTC())
	n := 0
	for _, p := range live.products {
		got := len(st.DAGs(p.ID))
		lg.Printf("airflow ingest %s: %d dag(s)", p.ID, got)
		n += got
	}
	return n, nil
}

func pollQuality(ctx context.Context, lg *log.Logger, st *store.Memory, eng *engine.Engine, live *liveAirflow, now time.Time) (int, error) {
	if live == nil || live.quality == nil {
		return 0, nil
	}
	checks, ids, err := live.quality.LatestChecks(ctx, live.products)
	if len(ids) > 0 {
		quality.Apply(st, eng, checks, ids, now)
		st.TouchQuality(time.Now().UTC())
		by := map[string]int{}
		for _, c := range checks {
			by[c.DataProductID]++
		}
		for _, id := range ids {
			lg.Printf("quality ingest %s: %d check(s)", id, by[id])
		}
	}
	return len(checks), err
}

func loadDemo(lg *log.Logger, st *store.Memory, eng *engine.Engine, now time.Time, configPath string) []string {
	path, err := demo.Resolve(configPath)
	if err != nil {
		lg.Fatalf("%v", err)
	}
	products, dags, checks, err := demo.LoadFile(path, now)
	if err != nil {
		lg.Fatalf("demo catalog %s: %v", path, err)
	}
	byDP := map[string][]domain.DAG{}
	for _, d := range dags {
		byDP[d.DataProductID] = append(byDP[d.DataProductID], d)
	}
	byC := map[string][]domain.Check{}
	for _, c := range checks {
		byC[c.DataProductID] = append(byC[c.DataProductID], c)
	}
	ids := make([]string, 0, len(products))
	for _, p := range products {
		st.UpsertProduct(p)
		st.SetDAGs(p.ID, byDP[p.ID])
		st.SetChecks(p.ID, byC[p.ID])
		ids = append(ids, p.ID)
	}
	eng.Recompute(now)
	st.TouchAstroRuns(now)
	st.TouchQuality(now)
	lg.Printf("demo catalog %s", path)
	return ids
}

func runWorkers(ctx context.Context, lg *log.Logger, st *store.Memory, eng *engine.Engine, alerts *alert.Router, iv intervals, mode string, live *liveAirflow) {
	clock := time.NewTicker(iv.clock)
	astroTick := time.NewTicker(iv.astro)
	tags := time.NewTicker(iv.tags)
	qualityTick := time.NewTicker(iv.quality)
	defer clock.Stop()
	defer astroTick.Stop()
	defer tags.Stop()
	defer qualityTick.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case t := <-clock.C:
			eng.Recompute(t)
			n := alerts.Scan(t)
			if len(n) > 0 {
				lg.Printf("clock tick: %d alert event(s)", len(n))
			}
		case t := <-astroTick.C:
			if mode == "demo" {
				st.TouchAstroRuns(t)
				lg.Printf("astro run poll (demo): snapshot already in memory, no HTTP")
				continue
			}
			if _, err := pollAirflow(ctx, lg, st, eng, live, t); err != nil {
				lg.Printf("astro run poll (live): %v", err)
			}
		case t := <-tags.C:
			st.TouchAstroTags(t)
			if mode == "demo" {
				lg.Printf("astro tag poll (demo): SLA tags already in the catalog")
				continue
			}
			if _, err := pollAirflow(ctx, lg, st, eng, live, t); err != nil {
				lg.Printf("astro tag poll (live): %v", err)
			}
		case t := <-qualityTick.C:
			if mode == "demo" {
				st.TouchQuality(t)
				lg.Printf("quality poll (demo): ValidX/dbt rows already in the catalog")
				continue
			}
			if _, err := pollQuality(ctx, lg, st, eng, live, t); err != nil {
				lg.Printf("quality poll (live): %v", err)
			}
		}
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func loadDotEnv() {
	for _, name := range []string{".env", ".env.local"} {
		raw, err := os.ReadFile(name)
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(raw), "\n") {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			k, v, ok := cutEq(line)
			if !ok || k == "" {
				continue
			}
			if os.Getenv(k) == "" {
				_ = os.Setenv(k, v)
			}
		}
	}
}

func cutEq(line string) (string, string, bool) {
	i := strings.IndexByte(line, '=')
	if i <= 0 {
		return "", "", false
	}
	k := strings.TrimSpace(line[:i])
	if strings.HasPrefix(k, "export ") {
		k = strings.TrimSpace(strings.TrimPrefix(k, "export "))
	}
	v := strings.TrimSpace(line[i+1:])
	if len(v) >= 2 {
		if q := v[0]; (q == '"' || q == '\'') && v[len(v)-1] == q {
			v = v[1 : len(v)-1]
		}
	}
	return k, v, true
}

func describeTable(t domain.QualityTable) string {
	if !t.Enabled {
		return "disabled"
	}
	return t.Database + "." + t.Schema + "." + t.Table
}
