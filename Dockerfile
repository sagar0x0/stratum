FROM golang:1.25-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG BINARY=storage
RUN CGO_ENABLED=0 GOOS=linux go build -o /stratum ./cmd/${BINARY}

FROM alpine:3.20

WORKDIR /app
COPY --from=builder /stratum /app/stratum

ENTRYPOINT ["/app/stratum"]
