# Shopping Sessions submission.
# The scorers are linux/amd64 and build this file from the repository root.
# No dataset is copied in. /data is mounted read-only at run time.
FROM rust:1.85-slim-bookworm AS build
WORKDIR /src
COPY solutions/rust/Cargo.toml solutions/rust/Cargo.lock /src/
COPY solutions/rust/src /src/src
RUN cargo build --release

FROM python:3.12-slim-bookworm

# The scorer runs the container as uid 1000.
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app

COPY solutions/rust/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY --from=build /src/target/release/shopping /app/shopping
COPY solutions/rust/pipeline/ /app/pipeline/
COPY solutions/rust/run.sh /app/run.sh
RUN chmod +x /app/run.sh /app/shopping \
    && rm -rf /app/pipeline/__pycache__

# Root is read-only at run time except /tmp (64 MB). run.sh points temp and
# home at /scratch. Logs go to stderr so stdout stays under the 50 MB cap.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SHOPPING_BIN=/app/shopping

WORKDIR /app
CMD ["/app/run.sh"]
