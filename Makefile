
test:
	@cargo test
format:
	@cargo fmt
lint:
	@cargo clippy
publish: test format lint
	@cargo run next-release && cargo publish --allow-dirty -p axum-webtools && cargo publish --allow-dirty -p axum-webtools-macros