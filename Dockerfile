FROM clux/muslrust:nightly-2023-08-17 AS builder

WORKDIR /app

COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock
COPY LICENSE LICENSE

RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release

ENV TARGET x86_64-unknown-linux-musl

COPY ./src ./src
RUN cargo build --release



FROM scratch

WORKDIR /app
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/dueue /app/dueue

CMD ["./dueue"]
