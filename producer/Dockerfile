# Producer Dockerfile
FROM golang:1.23.5-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -o producer .

# Final stage
FROM alpine:latest

WORKDIR /root/

COPY --from=builder /app/producer .

EXPOSE 8080

CMD ["./producer"]