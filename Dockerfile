FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /service

COPY pyproject.toml ./
COPY requirements-lock.txt requirements-dev-lock.txt ./
COPY app ./app
COPY alembic ./alembic
COPY alembic.ini ./
RUN pip install --no-cache-dir -c requirements-lock.txt . \
    && useradd --create-home --uid 10001 payments
USER payments
EXPOSE 8000
CMD ["uvicorn", "app.entrypoints.api.routes:app", "--host", "0.0.0.0", "--port", "8000"]

FROM runtime AS test
USER root
RUN pip install --no-cache-dir -c requirements-dev-lock.txt '.[test,dev]'
COPY tests ./tests
USER payments
CMD ["pytest", "-q", "-p", "no:cacheprovider"]
