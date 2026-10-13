# Event Consumer Service Dockerfile
FROM python:3.11-slim

# Prevent Python from writing .pyc files and enable unbuffered output
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

# Install dependencies in a single layer
COPY consumer/requirements.txt /app/consumer/requirements.txt
RUN pip install --no-cache-dir -r /app/consumer/requirements.txt

# Create volume/directory for processed logs
RUN mkdir -p /app/data

# Copy source code
COPY consumer/ /app/consumer/

# Run consumer service
CMD ["python", "-m", "consumer.app"]
