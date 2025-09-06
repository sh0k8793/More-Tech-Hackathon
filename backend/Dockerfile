FROM python:3.12-slim AS runtime
WORKDIR /app

COPY pyproject.toml /app
COPY ./src ./

#  Variables should be defined before their use (Usage of undefined variable '$PYTHONPATH') ???
ENV PYTHONPATH="/app:${PYTHONPATH}"
RUN pip install --no-cache-dir .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
