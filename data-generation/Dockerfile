FROM python:3.11-slim

# Install system dependencies for psycopg2 and pyarrow
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy data-generation code
COPY . .

# Set Python path to include data-generation directory
ENV PYTHONPATH=/app

# Set entrypoint
ENTRYPOINT ["python", "main.py"]

