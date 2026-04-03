# ---- Build stage ----
FROM golang:1.22-alpine AS builder

# Install CA certs for HTTPS calls to Cloudflare
RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy go.mod/go.sum first for caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY . .

# --- DEBUG STEP 1 ---
# List the files to make sure main.go was copied correctly.
RUN echo "--- Listing files after COPY ---" && ls -la

# Build static binary inside the current directory
RUN CGO_ENABLED=0 GOOS=linux go build -o ./exporter main.go

# ---- Runtime stage ----
FROM alpine:3.20

# Install CA certs and timezone data
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

# Create a non-root user
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

# Copy binary from the builder stage
COPY --from=builder /app/exporter /app/exporter

# Expose Prometheus port
EXPOSE 2112

# Run
ENTRYPOINT ["/app/exporter"]