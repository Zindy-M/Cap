# FILE: services/scylla-consumer/Dockerfile
# Docker configuration for ScyllaDB Consumer Service

FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

CMD ["python", "main.py"]