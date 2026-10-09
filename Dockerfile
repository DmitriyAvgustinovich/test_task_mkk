FROM python:3.12-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /service
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
RUN useradd --create-home --uid 10001 service
RUN chown service:service /service
COPY --chown=service:service app ./app
COPY --chown=service:service migrations ./migrations
COPY --chown=service:service alembic.ini ./
USER service
CMD ["uvicorn", "app.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]

FROM base AS test
USER root
COPY requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements-dev.txt
COPY --chown=service:service pyproject.toml ./
COPY --chown=service:service tests ./tests
COPY --chown=service:service scripts ./scripts
USER service
CMD ["python", "-m", "pytest", "-q"]
