# syntax=docker/dockerfile:1

FROM golang:1.26-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/queue ./cmd/queue
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/load-balancer ./cmd/load-balancer

FROM alpine:latest

RUN addgroup -S waitroom && adduser -S -G waitroom waitroom

COPY --from=build /out/queue /usr/local/bin/queue
COPY --from=build /out/worker /usr/local/bin/worker
COPY --from=build /out/load-balancer /usr/local/bin/load-balancer

USER waitroom

CMD ["queue"]
