setup-dev:
	@cargo install cargo-edit
update-deps:
	@cargo upgrade
sync-deps:
	@rm -f Cargo.lock && cargo update
test:
	@cargo test
format:
	@cargo fmt --all
lint:
	@cargo clippy --all-features -- -D warnings
pre-checks: test format lint
	@echo "All pre-checks passed"
publish-axum-webtools:
	@cargo run --bin next-release - tools && cargo publish --allow-dirty -p axum-webtools
publish-axum-webtools-macros:
	@cargo run --bin next-release - macros && cargo publish --allow-dirty -p axum-webtools-macros
publish-axum-webtools-pgsql-migrate:
	@cargo run --bin next-release - pgsql-migrate && cargo publish --allow-dirty -p axum-webtools-pgsql-migrate
publish-all: pre-checks publish-axum-webtools publish-axum-webtools-macros publish-axum-webtools-pgsql-migrate
	@echo "Published all packages"