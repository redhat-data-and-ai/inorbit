# web

Local test console. Same role as Rill’s [`web-local`](https://github.com/rilldata/rill/tree/main/web-local): source assets live here; production copies them into `internal/web/embed/dist` and the Go binary embeds that tree (`internal/web`, same idea as Rill [`cli/pkg/web`](https://github.com/rilldata/rill/blob/main/cli/pkg/web/handler.go)).

Theme and controls use a simple masthead, product cards, and Health / Freshness / Pipeline / Quality tiles. The page only calls `/v1`.

```bash
make ui.prepare   # copy web/ → internal/web/embed/dist
make run          # binary serves the embedded SPA at /
```

To drop the UI: remove `web/`, `internal/web/`, and the `web.Mount` call in `cmd/inorbit`.
