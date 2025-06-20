# Use Python 3.12 slim image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Copy project files
COPY . .

# Install UV for Python package management
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

# Install project dependencies using UV
RUN uv pip install -e .

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Default to running the processor
# Can be overridden with --command flag in Cloud Run Jobs
CMD ["uv", "run", "python", "-m", "src.pipeline.process_responses"]
