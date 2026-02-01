.PHONY: setup-dev update-deps sync-deps test format lint pre-checks \
        publish-axum-webtools publish-axum-webtools-macros \
        publish-axum-webtools-pgsql-migrate publish-axum-webtools-dlq-redrive \
        publish-all build build-release install-pgsql-migrate install-dlq-redrive

help: ## [Helper] Show help
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sort -t ':' -k2,2 | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

docker-build: ## [Dev] Build Docker images
	@docker compose build

update-deps: ## [Dev] Update dependencies
	@docker compose run --rm --no-deps task sh -c 'cargo upgrade'

sync-deps: ## [Dev] Sync dependencies
	@docker compose run --rm --no-deps task sh -c 'rm -f Cargo.lock && cargo update'

test: ## [Dev] Run tests
	@docker compose run --rm task sh -c 'cargo test --all-features'

format: ## [Dev] Format code
	@docker compose run --rm --no-deps task sh -c 'cargo fmt --all'

check: ## [Dev] Check code
	@docker compose run --rm --no-deps task sh -c 'cargo check --all-features'

lint: check ## [Dev] Lint code
	@docker compose run --rm --no-deps task sh -c 'cargo clippy --all-features -- -D warnings'

build: ## [Dev] Build code
	@docker compose run --rm --no-deps task sh -c 'cargo build --all-features'

build-release: ## [Dev] Build code in release mode
	@docker compose run --rm --no-deps task sh -c 'cargo build --release --all-features'

pre-checks: format lint test build build-release
	@echo "All pre-checks passed"

publish-axum-webtools:
	@cargo run --bin next-release - tools && cargo publish --allow-dirty -p axum-webtools

publish-axum-webtools-macros:
	@cargo run --bin next-release - macros && cargo publish --allow-dirty -p axum-webtools-macros

publish-axum-webtools-pgsql-migrate:
	@cargo run --bin next-release - pgsql-migrate && cargo publish --allow-dirty -p axum-webtools-pgsql-migrate

publish-axum-webtools-dlq-redrive:
	@cargo run --bin next-release - dlq-redrive && cargo publish --allow-dirty -p axum-webtools-dlq-redrive

publish-axum-webtools-dlq-redrive-docker-image:
	@docker build -t axum-webtools-dlq-redrive:latest --target prod-dlq-redrive -f ./Dockerfile .
	@docker tag axum-webtools-dlq-redrive:latest jslsolucoes/axum-webtools-dlq-redrive:latest
	@docker push jslsolucoes/axum-webtools-dlq-redrive:latest
	@echo "Published Docker image for axum-webtools-dlq-redrive"

publish-all: pre-checks publish-axum-webtools publish-axum-webtools-macros publish-axum-webtools-pgsql-migrate publish-axum-webtools-dlq-redrive
	@echo "Published all packages"