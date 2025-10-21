# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy everything
COPY . .

# Download dependencies and build
RUN go mod download
RUN go mod tidy
RUN go build -o api .

# Run stage
FROM alpine:latest

WORKDIR /app

# Copy binary
COPY --from=builder /app/api .

# Expose port
EXPOSE 8080

# Run
CMD ["./api"]
