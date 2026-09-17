# Deployment notes (not wired to production yet)

Live process: one Deployment, one container `inorbit`.

Environment:
- INORBIT_MODE=live
- INORBIT_HTTP_ADDR=:8080
- ASTRO_TOKEN (or ASTRO_API_TOKEN)
- ASTRO_ORG_URL
- INORBIT_ASTRO_DEPLOYMENT_ID
- ASTRO_ORGANIZATION_ID (optional)
- SNOWFLAKE_USER (InOrbit service account)
- SNOWFLAKE_ROLE
- SNOWFLAKE_PRIVATE_KEY_PATH
- SNOWFLAKE_ACCOUNT
- SLACK_BOT_TOKEN or SLACK_WEBHOOK_URL

Poll flags (or env later):
- -clock-seconds=15
- -astro-run-seconds=90
- -astro-tag-seconds=600
- -quality-seconds=120

Local first: `make run` (demo). Do not deploy until live ingest against Airflow and the warehouse is tested from a workstation.
