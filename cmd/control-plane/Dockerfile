FROM golang:1.26.0-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -v -o /bin/worker ./cmd/worker

FROM alpine:3.20

WORKDIR /app

COPY --from=builder /bin/worker ./cmd/worker

CMD ["/app/worker"]

