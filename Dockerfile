FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o /alert-engine ./cmd/alert-engine

FROM alpine:3.18

WORKDIR /
COPY --from=builder /alert-engine /alert-engine
COPY --from=builder /app/config /config

EXPOSE 8002

CMD ["/alert-engine"]
