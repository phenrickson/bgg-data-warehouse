# Use Python 3.12 slim image as base
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    build-essential \
    gnupg \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Install additional dependencies for Streamlit
ENV STREAMLIT_SERVER_PORT=8501
ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0
ENV STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

# Install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

# Copy project files
COPY pyproject.toml .
COPY Makefile .
COPY src/ src/
COPY config/ config/

# Install project dependencies
RUN /root/.cargo/bin/uv pip install -e .

# Create directories for data and credentials
RUN mkdir -p /app/data /app/credentials

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    GOOGLE_APPLICATION_CREDENTIALS=/app/credentials/service-account-key.json

# Create volume mount points
VOLUME ["/app/data", "/app/credentials"]

# Set default command
ENTRYPOINT ["python", "-m"]
CMD ["src.pipeline.fetch_data"]

# Health check (will be overridden by service-specific healthchecks in docker-compose)
HEALTHCHECK --interval=5m --timeout=3s \
    CMD curl -f http://localhost:${PORT:-8080}/health || exit 1

# Labels
LABEL org.opencontainers.image.source="https://github.com/yourusername/bgg-data-warehouse" \
      org.opencontainers.image.description="BoardGameGeek Data Warehouse Pipeline" \
      org.opencontainers.image.licenses="MIT"
