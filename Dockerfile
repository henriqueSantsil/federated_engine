FROM rust:1-bookworm AS builder

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates libgcc-s1 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home-dir /data --no-create-home federated \
    && mkdir -p /data \
    && chown federated:federated /data

COPY --from=builder /build/target/release/federated_engine /usr/local/bin/federated_engine

WORKDIR /data
VOLUME ["/data"]
EXPOSE 8080
USER federated

ENTRYPOINT ["/usr/local/bin/federated_engine"]
CMD ["--serve-http", "8080"]
