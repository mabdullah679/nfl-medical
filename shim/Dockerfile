FROM python:3.14-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY shim/requirements.txt /app/shim/requirements.txt
RUN pip install --no-cache-dir -r /app/shim/requirements.txt

COPY shim/ /app/shim/

# Role declarations are read at startup and must be present, or the app refuses to boot.
RUN test -d /app/shim/roles && test -n "$(ls /app/shim/roles/*.yaml)"

RUN useradd --create-home --uid 10001 shim && chown -R shim:shim /app
USER shim

EXPOSE 8096

# No credentials are baked in. Each endpoint's credential arrives at runtime from the
# compose env_file or, in production, from Secrets Manager via the task definition.

# /health answers 503 when a DEPENDENCY is down, which is not the shim being unhealthy.
# Container liveness therefore keys off /checks, which needs no backing store.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8096/checks', timeout=4).status==200 else 1)"

CMD ["python", "-m", "shim.main"]
