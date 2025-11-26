# Dockerfile for dbt transformations ECS task.
# Uses Python slim base image compatible with AWS Linux environments.
# Installs dbt-core with the Athena adapter and runs dbt transformations.

FROM python:3.11-slim

# Install system dependencies for building Python packages
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install dbt packages (dbt_utils)
COPY packages.yml .

# Copy dbt project files
COPY . .
RUN dbt deps

# Set Python path
ENV PYTHONPATH=/app

# Use container-specific profile that reads AWS credentials from IAM role
# The profile is copied as profiles.yml for dbt to find it
COPY profiles_container.yml /app/profiles.yml

# Entrypoint runs dbt with the container profile
ENTRYPOINT ["dbt", "run", "--profiles-dir", "/app", "--target", "dev"]

