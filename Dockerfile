FROM python:3.11-slim

# System dependencies for geopandas / GDAL and Java for PySpark
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gdal-bin \
    libgdal-dev \
    default-jdk-headless \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project source
COPY src/ ./src/
COPY config/ ./config/
COPY dags/ ./dags/

# Non-root user for security.
# chmod -R a+rX so appuser can read/execute files that had restrictive
# (0600) permissions on the host filesystem.
RUN useradd --create-home appuser \
    && chmod -R a+rX /app
USER appuser

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Default command — override in docker-compose per service
CMD ["python", "src/streaming/producer.py"]
