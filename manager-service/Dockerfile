# Use slim Python for smaller K8s image size 
FROM python:3.11-slim

# Install system dependencies for PostgreSQL driver
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy and install dependencies
COPY manager-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the shared logic and service code
COPY database/ ./database/
COPY manager-service/main.py .

# Set PYTHONPATH so 'import database' works
ENV PYTHONPATH=/app

# Security best practice: run as non-root
RUN adduser --disabled-password appuser
USER appuser

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]