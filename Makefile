
test:
	@cargo test
format:
	@cargo fmt
lint:
	@cargo clippy
pre-checks: test format lint
	@echo "All pre-checks passed"
publish-axum-webtools:
	@cargo run next-release tools && cargo publish --allow-dirty -p axum-webtools
publish-axum-webtools-macros:
	@cargo run next-release macros axum-webtools && cargo publish --allow-dirty -p axum-webtools-macros
publish-all: pre-checks publish-axum-webtools publish-axum-webtools-macros
	@echo "Published all packages"