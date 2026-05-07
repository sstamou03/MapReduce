FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY worker-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY database/ ./database/


COPY worker-service/worker.py .

ENV PYTHONPATH=/app

RUN adduser --disabled-password appuser
USER appuser

CMD ["python", "worker.py"]
