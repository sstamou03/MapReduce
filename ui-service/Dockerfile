FROM python:3.12-slim
WORKDIR /app

# 1. Copy and install requirements first (to use Docker cache)
COPY ui-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2. Copy the SHARED database folder into the container
COPY database/ database/

# 3. Copy the REST of the ui-service code
COPY ui-service/ .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]