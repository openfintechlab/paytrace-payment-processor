# PayTrace Payment Processor

## Introduction

This is a starter for building the PayTrace Payment Processor. It includes:

- Service bootstrap with lifecycle hooks
- Centralized configuration loading from environment variables and `.env`
- PostgreSQL connection initialization through SQLAlchemy
- RabbitMQ listener startup for domestic and cross-border payment request queues
- Test scaffolding with `pytest`

## Project Structure

```text
src/
  main.py                 # Service entrypoint and startup lifecycle
  utilities/ConfigLoader.py
  utilities/DBHelper.py
  utilities/Logging.py
  utilities/RabbitMQHelper.py
tests/
  test_config_loader.py
  test_rabbitmq_helper.py
```

## Prerequisites

- Python 3.11+ (recommended)
- `uv` installed
- PostgreSQL available for runtime startup checks
- RabbitMQ available for runtime queue listener startup

## Quick Start

### 1. Create local environment file

Copy the template environment file and edit values:

```bash
cp .env.example .env
```

Essential variables should be copied from `.env.example` and updated for your environment.

### 2. Fetch dependencies with `uv`

From the template root:

```bash
uv sync
```

If you want to install as an editable package instead:

```bash
uv pip install -e .
```

### 3. Run the processor

```bash
uv run python src/main.py
```

## Run with Docker

### 1. Build the container image

From the template root (`paytrace-sca-service-template`):

```bash
docker build -t pytrace-unittest-cimage:latest .
```

### 2. Run the container

Use the following command pattern to run the service with required environment variables:

```bash
docker run -d \
  --name paytrace-payment-processor \
  -e OFTL_LOG_LEVEL="INFO" \
  -e OFTL_LOG_FORMAT="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s" \
  -e OFTL_POSTGRESDB_USERNAME="admin" \
  -e OFTL_POSTGRESDB_PASSWORD="[CHANGE ME]" \
  -e OFTL_POSTGRESDB_HOST="host.docker.internal" \
  -e OFTL_POSTGRESDB_PORT="5432" \
  -e OFTL_POSTGRESDB_NAME="paytrace" \
  pytrace-unittest-cimage:latest
```

Or use a `.env` file with `--env-file`:

```bash
docker run -d \
  --name paytrace-payment-processor \
  --env-file .env \
  pytrace-unittest-cimage:latest
```

### 3. Verify container and endpoints

```bash
docker logs -f paytrace-unittest-cimage01
curl http://localhost:8081/sca/v1/
curl http://localhost:8081/_healthz
curl http://localhost:8081/_probe
```

## Configuration Reference

The core uses the following environment variables:

### Service Routing

- `OFTL_SCA_CONTEXT_ROOT`: Base API path (example: `/sca`)
- `OFTL_SCA_VERSION`: API version segment (example: `1`, exposed as `/v1`)
- `OFTL_SCA_HOST`: Bind host for Uvicorn (default fallback in code: `0.0.0.0`)
- `OFTL_SCA_PORT`: Bind port for Uvicorn (default fallback in code: `8081`)

Final route prefix is:

```text
${OFTL_SCA_CONTEXT_ROOT}/v${OFTL_SCA_VERSION}
```

Example with defaults in `.env.example`:

```text
/sca/v1
```

### Logging

- `OFTL_LOG_LEVEL`: Logger/Uvicorn log level (`INFO`, `DEBUG`, etc.)
- `OFTL_LOG_FORMAT`: Python logging format string

### Database (Required for startup DB initialization)

- `OFTL_POSTGRESDB_USERNAME`
- `OFTL_POSTGRESDB_PASSWORD`
- `OFTL_POSTGRESDB_HOST`
- `OFTL_POSTGRESDB_PORT`
- `OFTL_POSTGRESDB_NAME`

Optional:

- `OFTL_POSTGRESDB_POOLSIZE`: SQLAlchemy pool size (default: `10`)

### RabbitMQ (Required for queue listener startup)

- `OFTL_RABITMQ_HOST`
- `OFTL_RABITMQ_PORT`
- `OFTL_RABITMQ_USERNAME`
- `OFTL_RABITMQ_PASSWORD_SECRET`
- `OFTL_RABITMQ_VHOST`
- `OFTL_RABITMQ_HEARTBEAT`
- `OFTL_RABITMQ_BLOCKED_CONNECTION_TIMEOUT`
- `OFTL_RABITMQ_CONNECTION_ATTEMPTS`
- `OFTL_RABITMQ_CONN_RETRYCOUNT`: total number of application-level connection retries before exiting with code `99`
- `OFTL_RABITMQ_RETRY_DELAY`
- `OFTL_RABITMQ_SOCKET_TIMEOUT`
- `OFTL_RABITMQ_QUEUE_DURABLE`
- `OFTL_RABITMQ_PREFETCH_COUNT`
- `OFTL_RABITMQ_DOEMSTIC_REQUEST_QUEUE`: defaults to `CSV.PAYMENTS.DOMESTIC.REQ`
- `OFTL_RABITMQ_CROSS_BORDER_REQUEST_QUEUE`: defaults to `CSV.PAYMENTS.CROSS_BORDER.REQ`

When the service starts, it creates a persistent RabbitMQ listener and begins consuming from both configured request queues. If RabbitMQ cannot be reached after the configured `OFTL_RABITMQ_CONN_RETRYCOUNT` attempts, the processor exits with status code `99`.

## Default Routes

Registered in `src/routes/Routes.py`:

- `GET /` under the versioned service prefix (for example: `GET /sca/v1/`)
- `GET /_healthz` (public)
- `GET /_probe` (public)

Quick check:

```bash
curl http://localhost:8081/sca/v1/
curl http://localhost:8081/_healthz
curl http://localhost:8081/_probe
```

## Create a New Route

Add new route handlers inside `Routes._register_routes` in `src/routes/Routes.py`.

Example:

```python
@self.router.get("/transactions/ping")
async def transactions_ping() -> dict[str, str]:
    return {"service": "transactions", "status": "ok"}
```

With `OFTL_SCA_CONTEXT_ROOT=/sca` and `OFTL_SCA_VERSION=1`, this route becomes:

```text
GET /sca/v1/transactions/ping
```

## Testing

Run all tests:

```bash
uv run pytest tests -v
```

Run specific tests:

```bash
uv run pytest tests/test_config_loader.py -v
uv run pytest tests/test_routes.py -v
```

## Major Libraries Used

- `fastapi`: API framework
- `uvicorn`: ASGI server
- `pydantic`: data validation (FastAPI ecosystem)
- `environs`: environment variable parsing/loading
- `sqlalchemy`: database engine and ORM utilities
- `psycopg2-binary`: PostgreSQL driver
- `pytest`: test framework
- `httpx`: HTTP client used in test/runtime scenarios
