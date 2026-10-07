# Multi-stage build: First stage compiles the Go binaries
FROM golang:alpine AS builder

WORKDIR /app

# Download dependencies first (cached layer)
COPY go.mod go.sum ./
RUN go mod download

# Copy source code and build statically linked binaries
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/api ./cmd/api
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/worker ./cmd/worker

# Second stage: Minimal production image
FROM alpine:3.20

WORKDIR /app
COPY --from=builder /bin/api /bin/api
COPY --from=builder /bin/worker /bin/worker

# Default command placeholder
CMD ["/bin/api"]