# web

Local test console. Same role as Rill’s [`web-local`](https://github.com/rilldata/rill/tree/main/web-local): source assets live here; production copies them into `internal/web/embed/dist` and the Go binary embeds that tree (`internal/web`, same idea as Rill [`cli/pkg/web`](https://github.com/rilldata/rill/blob/main/cli/pkg/web/handler.go)).

Theme follows [PatternFly 5](https://www.patternfly.org/): white page, rounded product cards, search and pill filters, grid or list. The catalog and product pages default to Production pipelines; each product has an environment filter so Pre Prod does not mix with Production. Product pages show a primary-DAG Pipeline & Freshness strip, then a composite health scoreboard (donut, 7d/30d/3m/All trend, rule counts). Catalog chips show the numeric score for every status. Source-aligned products can list Fivetran connectors on Pipeline (count and types) and show connector type, connection count, and paused state on Lineage source chips. Tabs stay Overview · Pipeline · Quality · Lineage. Freshness is part of Pipeline. The page only calls `/v1`.

```bash
make ui.prepare   # copy web/ → internal/web/embed/dist
make run          # binary serves the embedded SPA at /
```

To drop the UI: remove `web/`, `internal/web/`, and the `web.Mount` call in `cmd/inorbit`.
