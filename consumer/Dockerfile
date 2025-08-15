# Stage 1: Build
FROM golang:1.24.5-alpine AS builder

WORKDIR /app

# Copy dependency files
COPY go.mod go.sum ./
COPY vendor ./vendor

# Copy source code
COPY . .

# Build (with vendored dependencies)
RUN CGO_ENABLED=0 GOOS=linux go build -mod=vendor -o consumer .

# Stage 2: Run
FROM alpine:latest

WORKDIR /root/

# Copy only the binary (no source code or vendor)
COPY --from=builder /app/consumer .

CMD ["./consumer"]