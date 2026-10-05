FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY provenance.py sources.py app.py selftest.py ./

# Non-root by default: this service needs no privilege, and the least-privilege
# posture the project requires elsewhere should not be abandoned here.
RUN useradd --create-home --uid 10001 stub
USER stub

EXPOSE 8098

HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:8098/health', timeout=4).status==200 else 1)"

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8098"]
