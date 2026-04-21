FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

ENV PYTHONPATH=/app/shared/contracts/src:/app/services/collector/src:/app/services/storage-consumer/src:/app/services/anomaly-engine/src:/app/services/contagion-engine/src:/app/services/api/src:/app/services/etl/src

COPY . /app
