# ---- build ----
FROM rust:1-slim-bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
      pkg-config libzmq3-dev build-essential \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
# Cache dependency compilation separately from source changes.
COPY Cargo.toml ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && cargo build --release && rm -rf src
COPY src ./src
RUN touch src/main.rs && cargo build --release

# ---- runtime ----
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
      libzmq5 ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /build/target/release/engine /usr/local/bin/engine
ENV RUST_LOG=info
ENTRYPOINT ["/usr/local/bin/engine"]
