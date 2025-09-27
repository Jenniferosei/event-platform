# Event-Driven Microservices Platform with Observability

This project is a learning journey to build an event-driven microservices platform that includes:

- Event ingestion and stream processing with Kafka
- Batch workflows with Airflow
- Observability with OpenTelemetry, metrics, and logs
- Reliability engineering with circuit breakers, chaos testing, and auto-remediation
- GitOps and policy-as-code practices

## Project Structure
- `infra/` → Infrastructure setup (local Kubernetes with k3d, etc.)
- `charts/` → Helm charts for deploying services
- `services/` → Microservices (demo app, stream processor, notification service, etc.)
- `scripts/` → Helper scripts for setup and automation

## Getting Started
1. Clone the repo:
   ```bash
   git clone <your-repo-url> event-platform
   cd event-platform
