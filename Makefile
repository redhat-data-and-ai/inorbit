.PHONY: test run run-live build ui.prepare

ui.prepare:
	rm -rf internal/web/embed/dist
	mkdir -p internal/web/embed/dist
	cp web/index.html web/app.js web/styles.css internal/web/embed/dist/

test: ui.prepare
	go test ./...

run: ui.prepare
	go run ./cmd/inorbit -mode=demo -config=configs/demo.json -addr=:8080

run-live: ui.prepare
	@set -a; \
	if [ -f .env ]; then . ./.env; fi; \
	if [ -f .env.local ]; then . ./.env.local; fi; \
	set +a; \
	if [ -z "$$ASTRO_TOKEN" ] && [ -z "$$ASTRO_API_TOKEN" ]; then \
	  echo "make run-live: ASTRO_TOKEN is empty — Airflow will 401 and the dashboard stays at 0 DAGs. Export ASTRO_TOKEN or put it in .env (gitignored)."; \
	fi; \
	cfg=configs/live.json; \
	if [ ! -f $$cfg ]; then cfg=configs/live.example.json; fi; \
	go run ./cmd/inorbit -mode=live -config=$$cfg -addr=:8080

build: ui.prepare
	mkdir -p bin
	go build -o bin/inorbit ./cmd/inorbit
