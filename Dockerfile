# One image, three processes — docker-compose.yml picks which one with `command:`.
# api runs `uvicorn app.main:app`, worker runs `python -m app.worker`, rate-service runs
# `uvicorn rate_service.main:app`. All three share the same dependencies, so one image is
# simpler to build and reason about than three near-identical ones.

FROM python:3.12-slim

RUN pip install --no-cache-dir uv

WORKDIR /app

# Copied separately from the app code so `uv sync` is cached and skipped on a code-only
# change — only a pyproject.toml/uv.lock edit invalidates this layer.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY app ./app
COPY rate_service ./rate_service

ENV PATH="/app/.venv/bin:$PATH"

# The API's own default port; worker and rate-service processes override this CMD via
# docker-compose.yml's `command:` instead of needing a second Dockerfile.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
