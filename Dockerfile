# Base image - Python 3.11 slim for smaller image size
FROM python:3.11-slim

# Set working directory inside the container
WORKDIR /app

# Install system dependencies needed for some Python packages
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first - Docker caches this layer
# If requirements don't change, Docker won't reinstall packages on rebuild
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the project
COPY . .

# Create necessary directories
RUN mkdir -p data/bronze data/silver data/gold logs

# Set Python path so imports work correctly
ENV PYTHONPATH=/app

# Default command - run the pipeline directly
CMD ["python", "-c", "from src.utils.config_loader import load_config; from src.ingestion.ingestor import run_ingestion; config = load_config(); run_ingestion(config)"]