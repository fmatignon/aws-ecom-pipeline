# Dockerfile for ingestion Lambda function.
# Uses Python slim base image with Lambda runtime interface client.
# Container images support up to 10GB, avoiding the 250MB zip limit.

# Define custom function directory
ARG FUNCTION_DIR="/var/task"

FROM python:3.11-slim

# Include global arg in this stage of the build
ARG FUNCTION_DIR

# Set working directory to function root directory
WORKDIR ${FUNCTION_DIR}

# Install build dependencies needed for pyarrow and other packages
RUN apt-get update && \
    apt-get install -y gcc g++ make cmake && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements and install dependencies including Lambda runtime interface client
COPY requirements.txt ${FUNCTION_DIR}
RUN pip install --no-cache-dir \
    awslambdaric \
    -r requirements.txt

# Copy application code
COPY . ${FUNCTION_DIR}

# Set runtime interface client as default command for the container runtime
ENTRYPOINT [ "/usr/local/bin/python", "-m", "awslambdaric" ]

# Pass the name of the function handler as an argument to the runtime
CMD [ "lambda_handler.lambda_handler" ]

