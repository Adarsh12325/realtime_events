# Event Producer Service Dockerfile
FROM python:3.11-slim

# Prevent Python from writing .pyc files and enable unbuffered logging
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

# Install dependencies in a single layer
COPY producer/requirements.txt /app/producer/requirements.txt
RUN pip install --no-cache-dir -r /app/producer/requirements.txt

# Copy source code
COPY producer/ /app/producer/

# Expose default HTTP port
EXPOSE 5000

# Healthcheck probe using built-in Python urllib
HEALTHCHECK --interval=10s --timeout=5s --start-period=15s --retries=3 \
  CMD python -c "import urllib.request, sys; sys.exit(0 if urllib.request.urlopen('http://localhost:5000/health').getcode() == 200 else 1)"

# Run producer service
CMD ["python", "-m", "producer.app"]
