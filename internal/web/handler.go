package web

import (
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"strings"
)

//go:embed all:embed
var distFS embed.FS

// StaticHandler serves the local console SPA (Rill cli/pkg/web.StaticHandler).
func StaticHandler() http.Handler {
	return http.FileServer(newUIAssetFS())
}

// Check if embed/dist exists. If not, serve the dummy embed/index.html.
func newUIAssetFS() http.FileSystem {
	_, err := distFS.ReadFile("embed/dist/index.html")
	if os.IsNotExist(err) || errors.Is(err, fs.ErrNotExist) {
		return assetFS(distFS, "embed")
	}
	return assetFS(distFS, "embed/dist")
}

func assetFS(embeddedFS embed.FS, dir string) http.FileSystem {
	subFS, err := fs.Sub(embeddedFS, dir)
	if err != nil {
		panic(fmt.Errorf("fs embed: %w", err))
	}
	return &SPARoutingFS{FileSystem: http.FS(subFS)}
}

// SPARoutingFS serves index.html when a path is missing (Rill cli/pkg/web).
type SPARoutingFS struct {
	FileSystem http.FileSystem
}

func (spaFS *SPARoutingFS) Open(name string) (http.File, error) {
	file, err := spaFS.FileSystem.Open(name)
	if err == nil {
		return file, nil
	}
	if errors.Is(err, fs.ErrNotExist) {
		return spaFS.FileSystem.Open("index.html")
	}
	return nil, err
}

// Mount serves the console at / and leaves /v1 and /healthz on next.
// Remove this package and the Mount() call in cmd/inorbit to drop the UI.
func Mount(next http.Handler) http.Handler {
	ui := StaticHandler()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		if p == "/healthz" || p == "/v1" || strings.HasPrefix(p, "/v1/") {
			next.ServeHTTP(w, r)
			return
		}
		if p == "/ui" || p == "/ui/" {
			http.Redirect(w, r, "/", http.StatusFound)
			return
		}
		ui.ServeHTTP(w, r)
	})
}
