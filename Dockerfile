FROM rust:1.93-alpine AS base
RUN apk --no-cache add ca-certificates cmake make gcc g++ musl-dev curl-dev zlib-static
WORKDIR /app

FROM base AS task
RUN rustup component add rustfmt
RUN rustup component add clippy
RUN cargo install cargo-edit

