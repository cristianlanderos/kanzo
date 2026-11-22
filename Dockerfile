# Builds Kanzo as one static program, then runs it from a small image.
FROM rust:1-alpine AS build
RUN apk add --no-cache build-base
WORKDIR /src
COPY . .
RUN cargo build --release --locked

FROM alpine:3.22
# git is for `kanzo sync`, curl for the update check.
RUN apk add --no-cache git curl ca-certificates
COPY --from=build /src/target/release/kanzo /usr/local/bin/kanzo
ENV KANZO_HOME=/data
VOLUME /data
EXPOSE 4242
# A new data folder listens on this machine only, which in a container is nobody. Open it
# once, when the folder is made; a config.toml that is already there is left alone.
CMD ["sh", "-c", "[ -f /data/config.toml ] || { kanzo init >/dev/null && sed -i 's/^listen = .*/listen = \"0.0.0.0\"/' /data/config.toml; }; exec kanzo --no-open"]
