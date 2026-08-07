FROM rust:1.80 as builder
WORKDIR /usr/src/app
COPY . .
RUN cargo build --release --bin management-service

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y libssl-dev ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/src/app/target/release/management-service /usr/local/bin/app
CMD ["app"]
