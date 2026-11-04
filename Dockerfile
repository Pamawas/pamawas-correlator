# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.27-alpine AS builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /app

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /pamawas-correlator .

ARG VERSION=""
RUN if [ -n "$VERSION" ]; then printf '%s\n' "$VERSION" > /app/version.txt; elif [ ! -f /app/version.txt ]; then printf 'dev-unknown\n' > /app/version.txt; fi

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /app
COPY --from=builder /pamawas-correlator .
COPY --from=builder /app/version.txt ./version.txt

EXPOSE 8080

CMD ["./pamawas-correlator"]
