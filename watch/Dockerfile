FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py ./

# Non-root: the watcher needs a network socket and nothing else.
RUN useradd --create-home --uid 10001 watch
USER watch

EXPOSE 8099

# Healthy means the loop is still ticking. A tick that found a breach is a
# healthy watcher doing its job.
HEALTHCHECK --interval=15s --timeout=5s --start-period=15s --retries=3 \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8099/', timeout=4).status==200 else 1)"

CMD ["python", "app.py"]
