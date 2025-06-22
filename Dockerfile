# Use Python 3.12 slim image
FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy project files
COPY . .

# Install UV for Python package management
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
RUN /root/.cargo/bin/uv install uv
RUN /root/.cargo/bin/uv pip install -e .

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PATH="/root/.cargo/bin:${PATH}"

# Default to running the processor
# Can be overridden with --command flag in Cloud Run Jobs
CMD ["uv", "run", "python", "-m", "src.pipeline.process_responses"]
