# Graph Alert Engine

A microservice-based alerting engine that monitors service graph metrics and generates real-time alerts based on anomaly detection, centrality analysis, and risk scoring.

## Architecture Overview

This service follows **Clean Architecture** principles with **Hexagonal Architecture** (Ports and Adapters) pattern, ensuring clear separation of concerns and testability.

### Key Components

- **Alert Engine**: Event-driven alerting system that processes service graph metrics
- **Risk Service**: Calculates risk scores using weighted metrics (RPS, latency, error rate, centrality)
- **Forecasting**: Hybrid forecasting adapter for anomaly detection using exponential smoothing
- **Graph Service**: Polls external graph service for real-time topology and centrality data
- **Webhook Notifier**: Delivers signed alert payloads to configured webhook endpoints
- **Circuit Breaker**: Resilience layer protecting against downstream failures

## Repository Structure

```
graph-alert-service/
├── cmd/
│   └── alert-engine/
│       └── main.go              # Application entry point, dependency injection
├── config/
│   └── config.go                # Environment-based configuration loader
├── internal/
│   ├── adapters/                # External integrations (Hexagonal Architecture)
│   │   ├── eventbus/            # In-memory event bus implementation
│   │   ├── forecasting/         # Hybrid forecasting adapter (anomaly detection)
│   │   ├── graphservice/        # Graph API client with polling, caching, resilience
│   │   ├── http/                # REST API handlers (health, alerts)
│   │   └── webhooks/            # Outbound webhook notifier with HMAC signing
│   ├── core/
│   │   ├── domain/              # Core business entities and value objects
│   │   │   ├── alert.go         # Alert domain model
│   │   │   ├── alert_metadata.go
│   │   │   ├── centrality.go   # Centrality scores (PageRank, Betweenness)
│   │   │   ├── events.go        # Domain events
│   │   │   ├── risk_score.go    # Risk calculation model
│   │   │   ├── service_node.go  # Service node representation
│   │   │   └── telemetry.go     # Metrics model
│   │   ├── ports/               # Interface definitions (dependency inversion)
│   │   │   ├── clock.go
│   │   │   ├── eventbus.go
│   │   │   ├── forecaster.go
│   │   │   ├── graph_provider.go
│   │   │   └── notifier.go
│   │   └── services/            # Core business logic
│   │       ├── alert_enrichment.go  # Enriches alerts with metadata
│   │       ├── availability_test.go
│   │       ├── restoration_test.go
│   │       ├── risk_service.go      # Risk calculation service
│   │       └── risk_service_test.go
├── k8s/
│   └── deployment.yaml          # Kubernetes deployment manifest
├── Dockerfile                    # Multi-stage Docker build
├── Makefile                      # Build and test automation
├── go.mod                        # Go module dependencies
└── README.md                     # This file
```

## Workflow

### 1. **Data Collection**
- Graph Poller fetches service topology and centrality metrics every 5 seconds from external graph service
- Circuit breaker protects against repeated failures
- Centrality scores (PageRank, Betweenness) are cached with configurable TTL

### 2. **Event Processing**
- In-memory event bus receives `ServiceMetricsReceived` events
- Risk Service subscribes to these events and calculates risk scores

### 3. **Risk Calculation**
```
Risk Score = (W1 × RPS_score) + (W2 × Latency_score) + (W3 × Error_score) + Centrality_score
```
- Forecasting adapter detects anomalies using exponential smoothing
- Centrality score amplifies risk for critical services

### 4. **Alert Generation**
- When risk exceeds threshold (`RISK_THRESHOLD`), an alert is created
- Alert includes service name, namespace, risk score, anomalies, and centrality data
- Alert Enrichment Service adds cluster, region, and environment metadata

### 5. **Alert Delivery**
- Webhook notifier sends signed payloads (HMAC-SHA256) to configured endpoints
- Alerts are transformed into structured JSON payloads

### 6. **API Exposure**
- REST API provides health checks and alert query endpoints
- Listens on port 8080 by default

## Setup & Running

### Prerequisites
- Go 1.22+
- Docker (optional)
- Kubernetes cluster (optional)

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `GRAPH_BASE_URL` | Base URL of the graph service API | **Required** |
| `GRAPH_TIMEOUT_MS` | HTTP timeout for graph requests | 2000 |
| `GRAPH_RETRY_MAX` | Max retry attempts | 2 |
| `CENTRALITY_CACHE_TTL_MS` | Cache TTL for centrality data | 30000 |
| `RISK_W1` | Weight for RPS score | 1.0 |
| `RISK_W2` | Weight for latency score | 1.0 |
| `RISK_W3` | Weight for error rate score | 1.0 |
| `RISK_THRESHOLD` | Alert threshold (0-100) | 60.0 |
| `WEBHOOK_TARGET_URLS` | Comma-separated webhook URLs | "" |
| `WEBHOOK_SECRET` | HMAC signing secret | "" |
| `CLUSTER_NAME` | Cluster identifier for alert metadata | "" |
| `REGION` | Region for alert metadata | "" |
| `ENVIRONMENT` | Environment (prod/stage/dev) | "" |

### Local Development

```bash
# Install dependencies
go mod download

# Run tests
make test

# Run with race detector
make test-race

# Format code
make fmt

# Lint
make lint

# Build binary
make build

# Run locally (set env vars first)
export GRAPH_BASE_URL=http://localhost:8081
export WEBHOOK_TARGET_URLS=http://localhost:9000/alerts
export WEBHOOK_SECRET=my-secret-key
make run
```

### Docker

```bash
# Build image
docker build -t graph-alert-service:latest .

# Run container
docker run -p 8080:8080 \
  -e GRAPH_BASE_URL=http://graph-service:8081 \
  -e WEBHOOK_TARGET_URLS=http://webhook-receiver:9000/alerts \
  -e WEBHOOK_SECRET=my-secret \
  graph-alert-service:latest
```

### Kubernetes

```bash
# Apply deployment
kubectl apply -f k8s/deployment.yaml

# Check status
kubectl get pods -l app=graph-alert-service

# View logs
kubectl logs -f deployment/graph-alert-service
```

## API Endpoints

- `GET /health` - Health check
- `GET /alerts` - Query recent alerts
- `GET /alerts/:id` - Get specific alert details

## Development Timeline & Merges

### Recent Development History

| Date | Commit | Description |
|------|--------|-------------|
| 2026-01-06 | 4cf8a5e | **Merge**: feature/alert-service into development |
| 2026-01-04 | afd6a70 | Add webhook improvements |
| 2026-01-04 | ec01e9f | Improve models structures |
| 2026-01-04 | da37db1 | Add alert enrichment |
| 2026-01-04 | cb5620c | Minor improvements in alert making |
| 2026-01-02 | 7516fbc | Add API server |
| 2025-12-31 | 40a1644 | Add graph poller |
| 2025-12-30 | 31001ce | Expand risk service |
| 2025-12-28 | 831ee77 | Add k8s deployment |
| 2025-12-27 | f7bb61c | Add docker image |

*Full commit history available via `git log --oneline`*

## Testing

The service includes comprehensive unit tests for core logic:

```bash
# Run all tests
go test ./...

# Run with coverage
go test ./... -cover

# Run specific package
go test ./internal/core/services/...
```

## Key Design Patterns

- **Clean Architecture**: Domain-centric design with dependency inversion
- **Hexagonal Architecture**: Adapters for external dependencies
- **Event-Driven**: Decoupled components via event bus
- **Repository Pattern**: Abstract data access
- **Circuit Breaker**: Resilience against cascading failures
- **Dependency Injection**: Explicit wiring in main.go

---

**Last Updated**: January 10, 2026
