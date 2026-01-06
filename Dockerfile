FROM golang:1.24.9-bookworm AS indexer

RUN set -ex; \
    apt-get update; \
    apt-get install -y build-essential bash git make gcc ca-certificates g++; \
    rm -rf /var/lib/apt/lists/*;
WORKDIR /indexer

# Download dependencies - will be cached if go.mod/go.sum doesn't change
COPY go.mod ./
COPY go.sum ./
RUN go mod download

# Set APP to control which binaries are built into the container
ARG APP

COPY . ./
RUN if [ -z "$APP" ]; then \
        make build-all; \
    else \
        APP="$APP" make build-app; \
    fi

# ----------------------
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=indexer /indexer/bin/* /usr/bin/

# no default here
ARG APP
ENV APP=${APP}

ENTRYPOINT ["/bin/sh","-c","exec /usr/bin/${APP:?set APP env to a service name} \"$@\"","--"]
CMD ["run"]
