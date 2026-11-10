FROM python:3.10.16-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app
WORKDIR /app
COPY docker/platform/requirements.lock /app/requirements.lock
RUN python -m pip install --no-cache-dir -r /app/requirements.lock \
 && python -m pip install --no-cache-dir --no-deps --index-url https://download.pytorch.org/whl/cpu 'torch==2.5.1' \
 && useradd --create-home --uid 10001 runner \
 && mkdir -p /work /artifacts && chown runner:runner /work /artifacts
COPY training /app/training
COPY config /app/config
COPY services/ranker /app/services/ranker
USER runner
ENV LOOP_WORK_ROOT=/work LOOP_ARTIFACT_ROOT=/artifacts
CMD ["python", "-m", "services.ranker.main", "selfcheck"]
