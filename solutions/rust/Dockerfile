# Shopping Sessions: Rust compute, PyArrow parquet write.
# Scorers are linux/amd64 and build this file natively.
FROM rust:1.85-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock /src/
COPY src /src/src
RUN cargo build --release

FROM python:3.12-slim-bookworm
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY --from=build /src/target/release/shopping /app/shopping
COPY pipeline/ /app/pipeline/
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh /app/shopping
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SHOPPING_BIN=/app/shopping
WORKDIR /app
CMD ["/app/run.sh"]
