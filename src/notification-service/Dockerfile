FROM rust:1.97 AS base
RUN cargo install sccache --version ^0.17
RUN cargo install cargo-chef --version ^0.1
ENV RUSTC_WRAPPER=sccache SCCACHE_DIR=/sccache

FROM base AS planner
WORKDIR /app/service
COPY src/betting-common /app/betting-common
COPY src/notification-service /app/service
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=$SCCACHE_DIR,sharing=locked \
    cargo chef prepare --recipe-path recipe.json

FROM base AS builder
WORKDIR /app/service
COPY --from=planner /app/service/recipe.json recipe.json
COPY src/betting-common /app/betting-common
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=$SCCACHE_DIR,sharing=locked \
    cargo chef cook --release --recipe-path recipe.json
COPY src/notification-service /app/service
ENV SQLX_OFFLINE=true
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=$SCCACHE_DIR,sharing=locked \
    cargo build --release --bin notification-service

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y libssl-dev ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/service/target/release/notification-service /usr/local/bin/server
CMD ["server"]
