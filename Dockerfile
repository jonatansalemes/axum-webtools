FROM rust:1.93-alpine AS base
RUN apk --no-cache add ca-certificates cmake make gcc g++ musl-dev curl-dev zlib-static
WORKDIR /app

FROM base AS task
RUN rustup component add rustfmt
RUN rustup component add clippy
RUN cargo install cargo-edit
RUN apk --no-cache add postgresql-client

FROM base AS builder-dlq-redrive
COPY ./dlq-redrive/Cargo.toml ./
RUN mkdir src
RUN echo "fn main() {}" > src/main.rs
RUN cargo build --release
RUN rm -rf ./src target/release/deps/dlq_redrive*
COPY ./dlq-redrive/src ./src
RUN cargo build --release

FROM scratch AS prod-dlq-redrive
COPY --from=builder-dlq-redrive /app/target/release/dlq-redrive ./dlq-redrive
USER 65534:65534
ENTRYPOINT ["./dlq-redrive"]
CMD ["-h"]

FROM base AS builder-pgsql-migrate
COPY ./pgsql-migrate/Cargo.toml ./
RUN mkdir src
RUN echo "fn main() {}" > src/main.rs
RUN cargo build --release
RUN rm -rf ./src target/release/deps/pgsql_migrate*
COPY ./pgsql-migrate/src ./src
RUN cargo build --release

FROM alpine:3.23 AS prod-pgsql-migrate-pg17
RUN apk add --no-cache \
    ca-certificates \
    libpq \
    postgresql17-client \
    && rm -rf /var/cache/apk/*
COPY --from=builder-pgsql-migrate /app/target/release/pgsql-migrate /usr/local/bin/pgsql-migrate
USER 65534:65534
ENTRYPOINT ["pgsql-migrate"]
CMD ["-h"]

FROM alpine:3.23 AS prod-pgsql-migrate-pg16
RUN apk add --no-cache \
    ca-certificates \
    libpq \
    postgresql16-client \
    && rm -rf /var/cache/apk/*
COPY --from=builder-pgsql-migrate /app/target/release/pgsql-migrate /usr/local/bin/pgsql-migrate
USER 65534:65534
ENTRYPOINT ["pgsql-migrate"]
CMD ["-h"]

