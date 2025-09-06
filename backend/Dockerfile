FROM python:3.12 AS builder

WORKDIR /app-build

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
COPY pyproject.toml ./app-build

RUN uv export --format=requirements-txt > requirements.txt

FROM python:3.12-slim AS runtime
WORKDIR /app

COPY --from=builder requirements.txt /app
COPY ./src ./

ENV PYTHONPATH="/app:${PYTHONPATH}"

RUN pip install --no-cache-dir -r /app/requirements.txt

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
