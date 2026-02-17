# Deployment

## Docker image

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir -e .
CMD ["ops-intel", "pipeline"]
```

Image included as `Dockerfile` in repository root.

## Runtime requirements

- Environment variables:
  - `FASTWEIGH_API_KEY`
  - `FASTWEIGH_TENANT_CONFIG`
  - `SMTP_USERNAME`/`SMTP_PASSWORD` (if email enabled)
- Persistent volume for `data/` and `output/`

## Suggested production deployment patterns

1. Container scheduled by Kubernetes CronJob
2. Windows Task Scheduler running `ops-intel pipeline`
3. Linux cron running container or virtualenv CLI

## CI/CD expectations

- Run unit tests and lint checks on PR
- Run schema guard in protected pipeline where API key is available
- Promote only when schema compatibility and tests pass
