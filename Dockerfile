# syntax=docker/dockerfile:1

# The build context is the workspace root (the directory containing go.work),
# not this service directory:
#
#   docker build -f order-service/Dockerfile .
#
# go.work is what makes the sibling modules (events, messaging) resolve from
# source. A standalone build (CI without the workspace) must drop go.work and
# pin those modules in go.mod instead.
FROM golang:1.26-alpine AS build

WORKDIR /src

# A static binary; no cgo runtime is needed in the final image.
# GOFLAGS=-mod=mod is deliberately NOT set: workspace mode rejects it.
ENV CGO_ENABLED=0

COPY events/ ./events/
COPY messaging/ ./messaging/
COPY order-service/ ./order-service/

# Rebuild the workspace with just the modules this image needs: the go.work in
# the repository lists every service, and go refuses to start when a listed
# directory is absent from the build context.
RUN rm -f go.work go.work.sum \
    && go work init ./events ./messaging ./order-service \
    && go build -trimpath -ldflags="-s -w" -o /out/order-service ./order-service

FROM alpine:3.22

# ca-certificates for TLS to Kafka/Postgres, tzdata so log timestamps match the
# configured timezone.
RUN apk add --no-cache ca-certificates tzdata \
    && adduser -D -u 10001 -H app

USER app
WORKDIR /app

COPY --from=build /out/order-service /app/order-service

EXPOSE 8080
ENTRYPOINT ["/app/order-service"]
