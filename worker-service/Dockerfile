FROM python:3.11-slim

# Install system dependencies for PostgreSQL (needed for the database module)
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY worker-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the shared database package
COPY database/ ./database/

# Copy the worker script
COPY worker-service/worker.py .

# Set PYTHONPATH so 'import database.xyz' works properly
ENV PYTHONPATH=/app

# Create a non-root user
RUN adduser --disabled-password appuser
USER appuser

# Workers just run the script and exit; no server required.
CMD ["python", "worker.py"]
