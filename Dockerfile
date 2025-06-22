# Use Python 3.12 slim image
FROM python:3.12-slim

# Ensure apt can fetch packages
RUN apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Install UV using pip
RUN pip install uv

# Copy project files
COPY . .

# Install project dependencies
RUN uv pip install -e .

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Default to running the processor
# Can be overridden with --command flag in Cloud Run Jobs
CMD ["uv", "run", "python", "-m", "src.pipeline.process_responses"]
