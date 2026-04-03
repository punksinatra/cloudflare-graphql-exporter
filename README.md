# Cloudflare GraphQL Exporter

A Prometheus exporter for Cloudflare metrics, built in Go and designed to run in Docker. Collects per-zone and account-level data via Cloudflare's GraphQL API and exposes them as Prometheus metrics.

## Features

- Fetches metrics for zones and accounts via Cloudflare GraphQL API.
- Provides detailed metrics including requests, bandwidth, page views, cache rate, threats, errors, HTTP versions, SSL protocols, and more.
- Supports per-country and per-browser breakdowns.
- Configurable refresh interval.
- Exposes metrics in Prometheus-compatible format.

## Requirements

- Go runtime (for building, optional if using Docker)
- Docker & Docker Compose
- Cloudflare API Token with appropriate permissions
- Cloudflare Account ID and Zone IDs

## Environment Variables

1. Copy the sample environment file:

```bash
cp env_sample .env
```

2. Edit `.env` and fill in your Cloudflare credentials:

| Variable | Required | Description | Default |
|----------|----------|-------------|---------|
| `CF_API_TOKEN` | Yes | Cloudflare API token | - |
| `CF_ACCOUNT_ID` | Yes | Cloudflare account ID | - |
| `CF_ZONE_IDS` | Yes | Comma-separated list of Cloudflare Zone IDs | - |
| `EXPORTER_ADDR` | No | Address/port to expose metrics | `:2112` |
| `REFRESH_MINUTES` | No | Refresh interval in minutes | `5` |
| `LOCAL_TZ` | No | Timezone for metrics | `UTC` |

> **Note:** The `env_sample` file is for public distribution and contains placeholders. Do **not** commit your real `.env` to version control.

## Usage

### Run with Docker Compose

```bash
docker-compose up -d
```

### Run manually

```bash
export CF_API_TOKEN=your_token
export CF_ACCOUNT_ID=your_account_id
export CF_ZONE_IDS=zone1,zone2
export EXPORTER_ADDR=:2112
export REFRESH_MINUTES=5
export LOCAL_TZ=UTC

go run main.go
```

### Metrics Endpoint

By default, metrics are exposed at:

```
http://localhost:2112/metrics
```

### Prometheus Example Scrape Config

```yaml
scrape_configs:
  - job_name: 'cloudflare_exporter'
    static_configs:
      - targets: ['localhost:2112']
```

## Logging

Logs are written to both stdout and `exporter.log` in the project directory.

## License

MIT License

