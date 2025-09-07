FROM python:3.12-slim AS runtime
WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

COPY pyproject.toml /app
COPY ./src ./

RUN uv sync

RUN apt-get update && apt-get install -y \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*


CMD ["uv","run","uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
