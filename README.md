# InOrbit

Go service for live data-product observability. A dashboard reads this HTTP API. Subscribe-based Slack alerts come from the same in-memory snapshot. Warehouse models stay for history, lineage, and catalog — not for every UI refresh.

**Which data products exist is config, not code.** Add an entry under `data_products` (and Airflow deployments under `astro.deployments`). The binary has no built-in product list.

## Local (no Airflow keys, no warehouse credentials)

```bash
make test
make run
```

`make run` loads [`configs/demo.json`](configs/demo.json): sample products **alpha** (freshness RED, quality fail) and **beta** (healthy). Edit that file — or point `-config` at your own catalog — to change names, DAGs, and checks. The SLA clock ticks every 15s in process. Airflow and quality pollers log only; they do not call the network in demo.

```bash
curl -s localhost:8080/healthz
curl -s localhost:8080/v1/meta
curl -s localhost:8080/v1/snapshots | python3 -m json.tool | head
curl -s localhost:8080/v1/data-products/alpha/freshness
curl -s localhost:8080/v1/data-products/alpha/health
curl -s -X POST localhost:8080/v1/subscriptions \
  -d '{"data_product_id":"alpha","audience":"developer","slack_channel":"#dp-alpha"}'
```

Open http://localhost:8080/ for the local test console (`web/` source, embedded via `internal/web`). It only calls `/v1`. Remove `web/`, `internal/web/`, and the `web.Mount` call in `cmd/inorbit` to drop it.

## Live (Airflow DAG status)

Demo stays the default. Live mode polls Airflow on startup, then on the run/tag intervals.

| Secret / env | Use |
|---|---|
| `ASTRO_TOKEN` or `ASTRO_API_TOKEN` | Bearer token for Airflow REST |
| `ASTRO_ORG_URL` | Org host, e.g. `https://<org-host>` |
| `INORBIT_ASTRO_DEPLOYMENT_ID` | Deployment id. Live URL is `{ASTRO_ORG_URL}/{id}/api/v2/dags` |
| `INORBIT_AIRFLOW_API_URL` | Optional override if you already have `{org}/{id}` or `{org}/{id}/api/v2` |
| `INORBIT_ASTRO_STAGE_DEPLOYMENT_ID` | Optional second deployment |

Copy [`configs/live.example.json`](configs/live.example.json) to `configs/live.json` (gitignored). Replace the sample `alpha` / `beta` products and `prod` / `stage` deployments with yours. Put the shared Snowflake **account**, role, and warehouse there. Tokens and `SNOWFLAKE_USER` stay in the environment.

```bash
export ASTRO_TOKEN=...
export ASTRO_ORG_URL=https://<org-host>
export INORBIT_ASTRO_DEPLOYMENT_ID=<prod-deployment-id>
export INORBIT_ASTRO_STAGE_DEPLOYMENT_ID=<stage-deployment-id>
export SNOWFLAKE_USER=...
make run-live
```

Add more objects under `astro.deployments` and `data_products` as needed. Matching ignores hyphens and underscores in tags and product ids. Optional `dag_ids` on a product pins those DAG ids even when tags do not match. If prod and stage are both listed, the same DAG id can appear twice (one row per deployment).

Open http://localhost:8080/ → a data product → **Pipeline**. Each row has **Open in Astro** (DAG overview URL). The same `astro_url` field is on `/v1/data-products/{id}/pipeline`.

Shape: `{ASTRO_ORG_URL}/{deployment_id}/dags/{dag_id}`. That is the DAG overview, not `/runs/{run_id}` (Astro DAG-level RBAC 403s the run page).

Snowflake SSO should prompt **once** when the process starts. Quality polls reuse that session. Restarting `make run-live` logs in again.

Live quality is per data product in config:

| Source | Table | When |
|---|---|---|
| Validation | `{database}.{schema}.{table}` (defaults `QUALITY.VALIDATION_RESULT`) | `quality.validx.enabled` |
| dbt / Elementary | `{database}.{schema}.{table}` (defaults `DBTLOGS.ELEMENTARY_TEST_RESULTS`) | `quality.dbt.enabled` |

Override `database`, `schema`, and `table` per product. Missing tables are skipped.

Health Score v2 is computed in process: only `FAILED` deducts, `UNKNOWN` dimensions have weight 0, coverage caps apply, and Astro freshness/pipeline virtual checks still contribute.

| Secret / env | Use |
|---|---|
| `SNOWFLAKE_USER` | User (or `snowflake.user_env`) |
| `SNOWFLAKE_PASSWORD` | Password auth |
| `SNOWFLAKE_PRIVATE_KEY_PATH` or `SNOWFLAKE_PRIVATE_KEY` | JWT key file or PEM |
| `SNOWFLAKE_ACCOUNT` / `SNOWFLAKE_ROLE` / `SNOWFLAKE_WAREHOUSE` | Optional overrides if omitted from `configs/live.json` |

```bash
make run-live
curl -s localhost:8080/v1/meta | python3 -m json.tool
curl -s localhost:8080/v1/data-products/alpha/pipeline | python3 -m json.tool | head
curl -s localhost:8080/v1/data-products/alpha/health
curl -s localhost:8080/v1/data-products/alpha/quality
```

Use the ids from **your** `data_products` list, not `alpha`, once you have replaced the example catalog.

## Compare live InOrbit to Airflow and warehouse marts

Marts lag and may filter paused/stage DAGs. Use them as lagged confirmation, not as the live source of truth. Skip products/tables that 403.

**UI:** http://localhost:8080/ → pick a data product → **Pipeline**. Each row expands: status, SLA, last/next run (local time), frequency/duration/SLA, and 7/30/90d reliability from recent Airflow dag runs. **Freshness** is the SLA clock. Hover a time for the exact timestamp. **Open in Astro** is on every Pipeline row.

**Is it realtime?** After `make run-live`:

1. `curl -s localhost:8080/v1/meta` — `mode` is `live`, `last_astro_run_poll` is within ~90s, `data_product_count` matches `data_products` in config.
2. Open each product → **Pipeline**. You should see DAGs whose tags or ids match that product, on the deployments you listed.
3. Click **Open in Astro**. Last success should match InOrbit’s `last_successful_at` (seconds-level).
4. Quality: warehouse tables in that product’s `quality` block should match the Quality tab. Missing objects are skipped (0 rows).

```sql
-- lagged marts, if you have them; replace database/product ids from your warehouse
select dag_id, astro_url, astro_deployment_name, dag_status,
       dag_freshness_status, last_successful_at
from {OBSERVABILITY_DB}.MARTS.PIPELINE_STATUS
where lower(coalesce(data_product_name, data_product_id)) in ('alpha', 'beta')
order by data_product_name, is_primary_dag desc, dag_id;
```

```bash
curl -s localhost:8080/v1/data-products/alpha/freshness | python3 -m json.tool
curl -s localhost:8080/v1/data-products/alpha/pipeline \
  | python3 -c 'import json,sys
for d in json.load(sys.stdin):
  print(d.get("astro_deployment_name"), d["dag_id"], d.get("dag_status"), d.get("last_successful_at"))'
```

## Poll intervals

Default poll intervals (override with flags):

| Loop | Default | I/O |
|---|---|---|
| SLA clock | 15s | none |
| Airflow latest runs | 90s | Airflow API per deployment |
| Airflow DAG tags | 10 min | Airflow API |
| Quality / dbt logs | 2 min | Warehouse watermark |

## 200 data products (memory and speed)

`GET /v1/meta` includes `scale`, projected from the live snapshot to **200** products:

- **Memory:** JSON snapshot × 200/N, plus ~3× heap copies and a 48 MiB process floor. Typical 80–200 KiB JSON per product → **~50–150 MiB RSS** at 200. The UI masthead shows the same estimate.
- **Airflow:** Listing cost is per deployment, not per product. Latest-run fetches grow with matched DAGs (8 workers). Keep the 90s run poll if matched DAGs stay in the low thousands; raise the interval if a poll overruns.
- **Quality:** One Snowflake session, sequential latest-run query per product. Missing tables are skipped. Budget **1–5 minutes** at 200 products, or raise `-quality-seconds`.
- **SLA clock:** in-process, no I/O; 15s stays cheap at 200.
